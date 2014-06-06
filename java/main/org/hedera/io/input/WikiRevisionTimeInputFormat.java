package org.hedera.io.input;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeConstants;
import org.joda.time.MutableDateTime;


public class WikiRevisionTimeInputFormat extends 
		WikiRevisionInputFormat<LongWritable, Text> {

	public static final String TIME_SCALE_OPT = "timescale";
	public static enum TimeScale {
		HOUR("hour"),
		DAY("day"),
		WEEK("week"),
		MONTH("month");		
		private final String val;		
		private TimeScale(String v) {val = v;}		
		public String toString() {
			return val;
		}

		public boolean equalsName(String name) {
			return (val.equals(name));
		}
	}

	private static Options opts = new Options();	
	private static final GnuParser parser = new GnuParser();
	private CommandLine options;

	private static void initOptions() {
		opts.addOption(TIME_SCALE_OPT, true, "The time scale used to coalesce the timeline");
	}

	public WikiRevisionTimeInputFormat() {
		super();
	}

	public WikiRevisionTimeInputFormat(String optString) {
		super();
		initOptions();
		if (optString != null && !optString.isEmpty()) {
			try {
				options = parser.parse(opts, optString.split(" "));
			} catch (org.apache.commons.cli.ParseException e) {
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp("[-" + TIME_SCALE_OPT + "]", opts);
				throw new RuntimeException(e);
			}	
		}
	}

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, 
			TaskAttemptContext context) {
		if (options != null) {
			if (!options.hasOption(TIME_SCALE_OPT)) {
				throw new RuntimeException("Must specify the time scale for RevisionDistant");
			} else {
				String scale = options.getOptionValue(TIME_SCALE_OPT);
				TimeScale ts = null;
				for (TimeScale t : TimeScale.values()) {
					if (t.equalsName(scale)) {
						ts = t;
					}
				}
				return new RevisionReader(ts);
			}
		} else throw new RuntimeException("Must specify the time scale for RevisionDistant");
	}

	// A flag that tells in which block the cursor is:
	// -1: EOF
	// 1 - outside the <page> tag
	// 2 - just passed the <page> tag but outside the <id> tag
	// 3 - just passed the <id> tag
	// 4 - just passed the </id> tag but outside the <revision> tag
	// 5 - just passed the (next) <revision>
	// 6 - just passed the <timestamp> inside the <revision>
	// 7 - just passed the </timestamp> but still inside the <revision></revision> block
	// 8 - just passed the </revision>
	// 9 - just passed the </page>
	public static class RevisionReader extends WikiRevisionReader<Text> {
		private static final Logger LOG = Logger.getLogger(RevisionReader.class); 		

		private static final byte[] DUMMY_REV = ("<revision beginningofpage=\"true\">"
				+ "<timestamp>1970-01-01T00:00:00Z</timestamp><text xml:space=\"preserve\">"
				+ "</text></revision>\n")
				.getBytes(StandardCharsets.UTF_8);

		// indicating the flow condition within [flag = 8]
		// -1 - Unmatched
		//  1 - Matched <revision> tag partially
		//  2 - Matched </page> tag partially
		//  3 - Matched both <revision> and </page> partially
		private int lastMatchTag = -1;

		private DataOutputBuffer pageHeader = new DataOutputBuffer();
		private DataOutputBuffer rev1Buf = new DataOutputBuffer();
		private DataOutputBuffer rev2Buf = new DataOutputBuffer();
		private DataOutputBuffer tmpBuf = new DataOutputBuffer();
		private DataOutputBuffer tsBuf = new DataOutputBuffer();
		private DataOutputBuffer keyBuf = new DataOutputBuffer();

		// remember the last time point
		private DateTime curTs;

		// remember the time scale constant
		private TimeScale timeScale;

		public RevisionReader() {
			super();
		}

		public RevisionReader(TimeScale ts) {
			super();
			this.timeScale = ts;
		}

		@Override
		public void initialize(InputSplit input, TaskAttemptContext tac)
				throws IOException, InterruptedException {
			super.initialize(input, tac);
			value = new Text();
		}

		@Override
		public STATE doWhenMatch() throws IOException, InterruptedException {
			if (flag == 9) {
				key.set(fsin.getPos() - rev2Buf.getLength() - END_PAGE.length);						
				value.set(pageHeader.getData(), 0, pageHeader.getLength() - START_REVISION.length);
				value.append(rev1Buf.getData(), 0, rev1Buf.getLength());
				value.append(rev2Buf.getData(), 0, rev1Buf.getLength());
				value.append(END_PAGE, 0, END_PAGE.length);
				// flush the last pair

				pageHeader.reset();	
				rev1Buf.reset();
				rev2Buf.reset();
				tmpBuf.reset();
				curTs = null;
				return STATE.STOP_TRUE;
			} 
			else if (flag == 7) {
				String ts = new String(tsBuf.getData(),0, tsBuf.getLength() - END_TIMESTAMP.length);
				tsBuf.reset();
				DateTime dt = roundup(ts);

				if (curTs != null && dt.isAfter(curTs)) {
					key.set(fsin.getPos() - tmpBuf.getLength() - rev2Buf.getLength());						
					value.set(pageHeader.getData(), 0, pageHeader.getLength() - START_REVISION.length);
					value.append(rev1Buf.getData(), 0, rev1Buf.getLength());
					value.append(rev2Buf.getData(), 0, rev1Buf.getLength());
					value.append(END_PAGE, 0, END_PAGE.length);

					rev1Buf.reset();
					rev1Buf.write(rev2Buf.getData());

					rev2Buf.reset();
					rev2Buf.write(tmpBuf.getData());		
					tmpBuf.reset();
					curTs = dt;	

					return STATE.STOP_TRUE;

				} else {
					rev2Buf.reset();
					rev2Buf.write(tmpBuf.getData());		
					tmpBuf.reset();
					curTs = dt;
				}
			}
			else if (flag == 4) {
				String pageId = new String(keyBuf.getData(), 0, keyBuf.getLength() - END_ID.length);
				key.set(Long.parseLong(pageId));	
				keyBuf.reset();
			}
			else if (flag == 2) {
				pageHeader.write(START_PAGE);
			}
			else if (flag == 5) {
				if (curTs == null) {							
					rev1Buf.write(DUMMY_REV);
				} 
				tmpBuf.write(START_REVISION);
			}
			else if (flag == 6) {
				tsBuf.reset();
			}
			else if (flag == -1) {
				pageHeader.reset();
				rev1Buf.reset();
				rev2Buf.reset();
				tmpBuf.reset();
				value.clear();
				return STATE.STOP_FALSE;
			}
			return STATE.CONTINUE;
		}

		private DateTime roundup(String timestamp) {
			MutableDateTime mdt = TIME_FORMAT.parseMutableDateTime(timestamp);

			if (timeScale == TimeScale.HOUR) {
				if (mdt.getMinuteOfHour() > 0 || mdt.getSecondOfMinute() > 0 || mdt.getMillisOfSecond() > 0) {
					mdt.addHours(1);
				}
				mdt.setMinuteOfHour(0);
				mdt.setSecondOfMinute(0);
				mdt.setMillisOfSecond(0);


			} else if (timeScale == TimeScale.DAY) {
				if (mdt.getHourOfDay() > 1 || mdt.getMinuteOfHour() > 0 || mdt.getSecondOfMinute() > 0 
						|| mdt.getMillisOfSecond() > 0) {
					mdt.addDays(1);
				}
				mdt.setHourOfDay(1);
				mdt.setMinuteOfHour(0);
				mdt.setSecondOfMinute(0);
				mdt.setMillisOfSecond(0);

			} else if (timeScale == TimeScale.WEEK) {
				if (mdt.getDayOfWeek() > 1 || mdt.getHourOfDay() > 1 || mdt.getMinuteOfHour() > 0 
						|| mdt.getSecondOfMinute() > 0 || mdt.getMillisOfSecond() > 0) {
					mdt.addWeeks(1);								
				}
				mdt.setDayOfWeek(DateTimeConstants.MONDAY);
				mdt.setHourOfDay(1);
				mdt.setMinuteOfHour(0);
				mdt.setSecondOfMinute(0);
				mdt.setMillisOfSecond(0);
			} else if (timeScale == TimeScale.MONTH) {
				if (mdt.getDayOfMonth() > 1 || mdt.getHourOfDay() > 1 || mdt.getMinuteOfHour() > 0 
						|| mdt.getSecondOfMinute() > 0 || mdt.getMillisOfSecond() > 0) {
					mdt.addWeeks(1);								
				}
				mdt.setDayOfMonth(1);
				mdt.setHourOfDay(1);
				mdt.setMinuteOfHour(0);
				mdt.setSecondOfMinute(0);
				mdt.setMillisOfSecond(0);
			}
			return mdt.toDateTimeISO();
		}

		protected boolean readUntilMatch() throws IOException {
			if (buf == null && pos.length != 2)
				throw new IOException("Internal buffer corrupted.");
			int i = 0;
			while (true) {
				if (pos[0] == pos[1]) {				
					pos[1] = (compressed) ? ((CompressionInputStream)fsin).read(buf) :
						((FSDataInputStream)fsin).read(buf);
					LOG.info(pos[1] + " bytes read from the stream...");
					pos[0] = 0;
					if (pos[1] == -1) {
						return false;
					}
				} 
				while (pos[0] < pos[1]) {
					byte b = buf[pos[0]];
					pos[0]++;
					// ignore every character until reaching a new page
					if (flag == 1 || flag == 9) {
						if (b == START_PAGE[i]) {
							i++;
							if (i >= START_PAGE.length) {
								flag = 2;
								return true;
							}
						} else i = 0;
					}

					// put everything between <page> tag and the first <id> tag into pageHeader
					else if (flag == 2) {
						if (b == START_ID[i]) {
							i++;
						} else i = 0;
						pageHeader.write(b);
						if (i >= START_ID.length) {
							flag = 3;
							return true;
						}
					}

					// put everything in <id></id> block into pageHeader and keyBuf
					else if (flag == 3) {
						if (b == END_ID[i]) {
							i++;
						} else i = 0;
						pageHeader.write(b);
						keyBuf.write(b);
						if (i >= END_ID.length) {
							flag = 4;
							return true;
						}
					}

					// put everything between </id> tag and the first <revision> tag into pageHeader
					else if (flag == 4) {
						if (b == START_REVISION[i]) {
							i++;
						} else i = 0;
						pageHeader.write(b);
						if (i >= START_REVISION.length) {
							flag = 5;
							return true;
						}
					}

					// everything between <revision> and <timestamp> goes into tmpBuf buffer
					else if (flag == 5) {
						if (b == START_TIMESTAMP[i]) {
							i++;
						} else i = 0;
						tmpBuf.write(b);					
						if (i >= START_TIMESTAMP.length) {
							flag = 6;
							// tsBuf.reset();
							return true;
						}
					}

					// everything between <timestamp> </timestamp> block goes into tmpBuf and tsBuf buffers
					else if (flag == 6) {
						if (b == END_TIMESTAMP[i]) {
							i++;
						} else i = 0;
						tsBuf.write(b);
						tmpBuf.write(b);
						if (i >= END_TIMESTAMP.length) {
							flag = 7;
							return true;
						}
					}

					// everything up to </revision> goes into rev2Buf
					else if (flag == 7) {
						if (b == END_REVISION[i]) {
							i++;
						} else i = 0;
						rev2Buf.write(b);
						if (i >= END_REVISION.length) {
							flag = 8;
							return true;
						}
					}

					// Note that flag 6 can be the signal of a new revision inside one old page
					else if (flag == 8) {
						int curMatch = 0;				
						if ((i < END_PAGE.length && b == END_PAGE[i]) 
								&& (i < START_REVISION.length && b == START_REVISION[i])) {
							curMatch = 3;
						} else if (i < END_PAGE.length && b == END_PAGE[i]) {
							curMatch = 2;
						} else if (i < START_REVISION.length && b == START_REVISION[i]) {
							curMatch = 1;
						}				
						if (curMatch > 0 && (i == 0 || lastMatchTag == 3 || curMatch == lastMatchTag)) {					
							i++;			
							lastMatchTag = curMatch;
						} else i = 0;
						if ((lastMatchTag == 2 || lastMatchTag == 3) && i >= END_PAGE.length) {
							flag = 9;
							lastMatchTag = -1;
							return true;							
						} else if ((lastMatchTag == 1 || lastMatchTag == 3) && i >= START_REVISION.length) {
							flag = 5;
							lastMatchTag = -1;
							return true;
						}				
					} 
				}
			}
		}
	}
}
