package org.hedera.io.input;


import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.hedera.io.WikiRevisionWritable;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

public class WikiRevisionPageInputFormat extends WikiRevisionInputFormat<WikiRevisionWritable> {
	
	@Override
	public RecordReader<LongWritable, WikiRevisionWritable> createRecordReader(InputSplit split, 
			TaskAttemptContext context) {
		return new RevisionReader();
	}
	
	/**
	 * Read each revision of Wikipedia page and transform into a WikiRevisionWritable object
	 * @author tuan
	 */
	public static class RevisionReader extends RecordReader<LongWritable, WikiRevisionWritable> {

		public static final byte[] START_TEXT = "<text xml:space=\"preserve\">"
				.getBytes(StandardCharsets.UTF_8);
		public static final byte[] END_TEXT = "</text>".getBytes(StandardCharsets.UTF_8);
		
		public static final byte[] START_PARENT_ID = "<text xml:space=\"preserve\">"
				.getBytes(StandardCharsets.UTF_8);
		public static final byte[] END_PARENT_ID = "</text>".getBytes(StandardCharsets.UTF_8);
		
		private static final DateTimeFormatter dtf = ISODateTimeFormat.dateTimeNoMillis();
		
		private long start;
		private long end;

		// A flag that tells in which block the cursor is:
		// -1: EOF
		// 1 - outside the <page> tag
		// 2 - just passed the <page> tag but outside the <title>
		// 3 - just passed the <title> tag
		// 4 - just passed the </title> tag but outside the <id>
		// 5 - just passed the (page's) <id>
		// 6 - just passed the </id> tag but outside the <revision>	
		// 7 - just passed the (next) <revision>
		// 8 - just passed the inner <id> tag inside <revision>
		// 9 - just passed the inner </id> tag inside <revision>
		// 10 - just passed the <timestamp>
		// 11 - just passed the </timestamp> tag
		// 12 - just passed the <parentId>
		// 13 - just passed the </parentId> tag
		// 14 - just passed the <text> tag
		// 15 - just passed the </text> tag
		// 16 - just passed the </revision>
		// 17 - just passed the </page>
		private byte flag;

		// compression mode checking
		private boolean compressed = false;

		// indicating the flow condition within [flag = 16]
		// -1 - Unmatched
		//  1 - Matched <revision> tag partially
		//  2 - Matched </page> tag partially
		//  3 - Matched both <revision> and </page> partially
		private int revOrPage = -1;
		
		// indicating the flow condition within [flag = 9]
		// -1 - Unmatched
		//  1 - Matched <parentId> tag partially
		//  2 - Matched <timestamp> tag partially
		//  3 - Matched both <parentId> and <timestamp> partially
		private int parOrTs = -1;
		
		// a direct buffer to improve the local IO performance
		private byte[] buf = new byte[134217728];
		private int[] pos = new int[2];

		private Seekable fsin;

		
		// We now convert and cache everything from pageHeader to the followin global variables
		// NOTE: they all need to be synchronized with pageHeader !!
		// private DataOutputBuffer pageHeader = new DataOutputBuffer();
		private DataOutputBuffer pageTitle = new DataOutputBuffer();
		private String title;
	    //////////////////////////////////////////////////////////////
		// END PageHeader variables
		//////////////////////////////////////////////////////////////
		
		private DataOutputBuffer revBuf = new DataOutputBuffer();
		private long revId;
		
		private DataOutputBuffer parBuf = new DataOutputBuffer();
		private long parId;
		
		private DataOutputBuffer keyBuf = new DataOutputBuffer();
		private long pageId;
		
		private DataOutputBuffer timestampBuf = new DataOutputBuffer();
		private long timestamp;
		
		private DataOutputBuffer contentBuf = new DataOutputBuffer();

		private final LongWritable key = new LongWritable();
		private final WikiRevisionWritable value = new WikiRevisionWritable();

		@Override
		public void initialize(InputSplit input, TaskAttemptContext tac)
				throws IOException, InterruptedException {

			// config xmlinput properties to support bzip2 splitting
			Configuration conf = tac.getConfiguration();
			setBlockSize(conf);
			
			FileSplit split = (FileSplit) input;
			start = split.getStart();
			Path file = split.getPath();

			CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(conf);
			CompressionCodec codec = compressionCodecs.getCodec(file);

			FileSystem fs = file.getFileSystem(conf);

			if (codec != null) { // file is compressed
				compressed = true;
				// fsin = new FSDataInputStream(codec.createInputStream(fs.open(file)));
				CompressionInputStream cis = codec.createInputStream(fs.open(file));

				cis.skip(start - 1);

				fsin = cis;
			} else { // file is uncompressed	
				compressed = false;
				fsin = fs.open(file);
				fsin.seek(start);
			}

			flag = 1;
			pos[0] = pos[1] = 0;
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (fsin.getPos() < end) {
				while (readUntilMatch()) {  
					if (flag == 17) {
						keyBuf.reset();
						pageId = -1;
						pageTitle.reset();
						title = null;
						value.clear();
					} 
					else if (flag == 16) {	
						value.setPageId(pageId);
						value.setPageTitle(title);
						value.setRevisionId(revId);
						value.setParentId(parId);
						value.setTimestamp(timestamp);
						value.loadText(contentBuf.getData(), 0, contentBuf.getLength() 
								- END_TEXT.length);
						
						// reset written values
						contentBuf.reset();
						revId = -1;			
						parId = -1;					
						timestamp = -1;
						
						return true;
					}
					else if (flag == 13) {
						String parIdStr = new String(parBuf.getData(), 0, parBuf.getLength() 
								- END_PARENT_ID.length);
						parId = Long.parseLong(parIdStr);
						parBuf.reset();
					}
					else if (flag == 11) {
						String ts = new String(timestampBuf.getData(), 0, timestampBuf.getLength() 
								- END_TIMESTAMP.length);
						timestamp = dtf.parseMillis(ts);
						timestampBuf.reset();
					}
					else if (flag == 9) {
						String idStr = new String(revBuf.getData(), 0, revBuf.getLength()
								- END_ID.length);
						revId = Long.parseLong(idStr);
						revBuf.reset();
					}
					else if (flag == 6) {
						String idStr = new String(keyBuf.getData(), 0, keyBuf.getLength()
								- END_ID.length);
						pageId = Long.parseLong(idStr);
						key.set(pageId);
						keyBuf.reset();
					}
					else if (flag == 4) {
						title = new String(pageTitle.getData(), 0, pageTitle.getLength()
								- END_TITLE.length);
						pageTitle.reset();
					}
					else if (flag == -1) {
						return false;
					}
				}
			}
			return false;
		}		

		@Override
		public LongWritable getCurrentKey() throws IOException, InterruptedException {
			return key;
		}

		@Override
		public WikiRevisionWritable getCurrentValue() throws IOException, InterruptedException {
			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return (fsin.getPos() - start) / (float) (end - start);
		}

		@Override
		public void close() throws IOException {
			if (compressed) {
				((CompressionInputStream)fsin).close();
			} else {
				((FSDataInputStream)fsin).close();
			}
		}
		
		// Scan the tags in SAX manner. Return at every legit tag and inform the program via 
		// the global flag. Flush into the caches if necessary
		private boolean readUntilMatch() throws IOException {
			if (buf == null && pos.length != 2)
				throw new IOException("Internal buffer corrupted.");
			int i = 0;
			while (true) {
				if (pos[0] == pos[1]) {				
					pos[1] = (compressed) ? ((CompressionInputStream)fsin).read(buf) :
						((FSDataInputStream)fsin).read(buf);
					pos[0] = 0;
					if (pos[1] == -1) {
						return false;
					}
				} 
				while (pos[0] < pos[1]) {
					byte b = buf[pos[0]];
					pos[0]++;

					// ignore every character until reaching a new page
					if (flag == 1 || flag == 15) {
						if (b == START_PAGE[i]) {
							i++;
							if (i >= START_PAGE.length) {
								flag = 2;
								return true;
							}
						} else i = 0;
					}
					
					else if (flag == 2) {
						if (b == START_TITLE[i]) {
							i++;
						} else i = 0;
						if (i >= START_TITLE.length) {
							flag = 3;
							return true;
						}
					}
					
					// put everything between <title></title> block into title
					else if (flag == 3) {
						if (b == END_TITLE[i]) {
							i++;
						} else i = 0;
						pageTitle.write(b);
						if (i >= END_TITLE.length) {
							flag = 4;
							return true;
						}
					}
					
					else if (flag == 4) {
						if (b == START_ID[i]) {
							i++;
						} else i = 0;
						if (i >= START_ID.length) {
							flag = 5;
							return true;
						}
					}

					// put everything in outer <id></id> block into keyBuf
					else if (flag == 5) {
						if (b == END_ID[i]) {
							i++;
						} else i = 0;
						keyBuf.write(b);
						if (i >= END_ID.length) {
							flag = 6;
							return true;
						}
					}
					
					else if (flag == 6) {
						if (b == START_REVISION[i]) {
							i++;
						} else i = 0;
						if (i >= START_REVISION.length) {
							flag = 7;
							return true;
						}
					}
					
					// inside <revision></revision> block, first check for id
					else if (flag == 7) {
						if (b == START_ID[i]) {
							i++;
						} else i = 0;
						if (i >= START_ID.length) {
							flag = 8;
							return true;
						}
					}
					
					// everything inside the inner <id></id> block goes to revision buffer
					else if (flag == 8) {
						if (b == END_ID[i]) {
							i++;
						} else i = 0;
						revBuf.write(b);
						if (i >= END_ID.length) {
							flag = 9;
							return true;
						}
					}
					
					// after the inner <id>, check for either <timestamp> or <parentId>
					else if (flag == 9) {
						int curMatch = 0;				
						if ((i < START_PARENT_ID.length && b == START_PARENT_ID[i]) 
								&& (i < START_TIMESTAMP.length && b == START_TIMESTAMP[i])) {
							curMatch = 3;
						} else if (i < START_PARENT_ID.length && b == START_PARENT_ID[i]) {
							curMatch = 1;
						} else if (i < START_TIMESTAMP.length && b == START_TIMESTAMP[i]) {
							curMatch = 2;
						}				
						if (curMatch > 0 && (i == 0 || parOrTs == 3 || curMatch == parOrTs)) {					
							i++;			
							parOrTs = curMatch;
						} else i = 0;
						if ((parOrTs == 2 || parOrTs == 3) && i >= START_TIMESTAMP.length) {
							flag = 10;
							parOrTs = -1;
							return true;							
						} else if ((parOrTs == 1 || parOrTs == 3) && i >= START_PARENT_ID.length) {
							flag = 12;
							parOrTs = -1;
							return true;
						}		
					}
					
					// inside <timestamp></timestamp> block everything goes to timestamp buffer
					else if (flag == 10) {
						if (b == END_TIMESTAMP[i]) {
							i++;
						} else i = 0;
						timestampBuf.write(b);
						if (i >= END_TIMESTAMP.length) {
							flag = 11;
							return true;
						}
					}
					
					// inside <parentId></parentId> block everything goes to parentId buffer
					else if (flag == 12) {
						if (b == END_PARENT_ID[i]) {
							i++;
						} else i = 0;
						parBuf.write(b);
						if (i >= END_PARENT_ID.length) {
							flag = 13;
							return true;
						}
					}
					
					// after the </parentId>, search for <timestamp>
					else if (flag == 13) {
						if (b == START_TIMESTAMP[i]) {
							i++;
						} else i = 0;
						if (i >= START_TIMESTAMP.length) {
							flag = 10;
							return true;
						}
					}
					
					// after the </timestamp>, check for <text>
					else if (flag == 11) {
						if (b == START_TEXT[i]) {
							i++;
						} else i = 0;
						if (i >= START_TEXT.length) {
							flag = 14;
							return true;
						}
					}
					
					// inside <text></text> block everything goes to content buffer
					else if (flag == 14) {
						if (b == END_TEXT[i]) {
							i++;
						} else i = 0;
						contentBuf.write(b);
						if (i >= END_TEXT.length) {
							flag = 15;
							return true;
						}
					}
					
					// look for the closing </revision>
					else if (flag == 15) {
						if (b == END_REVISION[i]) {
							i++;
						} else i = 0;
						if (i >= END_REVISION.length) {
							flag = 16;
							return true;
						}
					}

					// Flag 16 can be the signal of a new record inside one old page
					else if (flag == 16) {
						int curMatch = 0;				
						if ((i < END_PAGE.length && b == END_PAGE[i]) 
								&& (i < START_REVISION.length && b == START_REVISION[i])) {
							curMatch = 3;
						} else if (i < END_PAGE.length && b == END_PAGE[i]) {
							curMatch = 2;
						} else if (i < START_REVISION.length && b == START_REVISION[i]) {
							curMatch = 1;
						}				
						if (curMatch > 0 && (i == 0 || revOrPage == 3 || curMatch == revOrPage)) {					
							i++;			
							revOrPage = curMatch;
						} else i = 0;
						if ((revOrPage == 2 || revOrPage == 3) && i >= END_PAGE.length) {
							flag = 17;
							revOrPage = -1;
							return true;							
						} else if ((revOrPage == 1 || revOrPage == 3) && i >= START_REVISION.length) {
							flag = 7;
							revOrPage = -1;
							return true;
						}				
					} 
				}		
			}
		}
	}
}
