package org.hedera.mapreduce.io.wikipedia;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;
import org.hedera.mapreduce.io.wikipedia.WikipediaRevisionInputFormat.TimeScale;
import org.joda.time.DateTime;
import org.joda.time.MutableDateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import static org.hedera.mapreduce.io.wikipedia.WikipediaRevisionInputFormat.END_PAGE;
import static org.hedera.mapreduce.io.wikipedia.WikipediaRevisionInputFormat.END_REVISION;
import static org.hedera.mapreduce.io.wikipedia.WikipediaRevisionInputFormat.START_PAGE;
import static org.hedera.mapreduce.io.wikipedia.WikipediaRevisionInputFormat.START_REVISION;

public class WikiRevisionDiffReader extends RecordReader<LongWritable, Text> {
	private static final Logger LOG = Logger.getLogger(WikiRevisionPairRecordReader.class); 		

	public static final String START_TIMESTAMP_TAG = "<timestamp>";
	public static final String END_TIMESTAMP_TAG = "</timestamp>";
	public static final byte[] START_TIMESTAMP = START_TIMESTAMP_TAG.getBytes(StandardCharsets.UTF_8);
	public static final byte[] END_TIMESTAMP = END_TIMESTAMP_TAG.getBytes(StandardCharsets.UTF_8);
	
	private static final byte[] DUMMY_REV = ("<revision beginningofpage=\"true\">"
			+ "<timestamp>1970-01-01T00:00:00Z</timestamp><text xml:space=\"preserve\">"
			+ "</text></revision>\n")
			.getBytes(StandardCharsets.UTF_8);

	private long start;
	private long end;

	// A flag that tells in which block the cursor is:
	// -1: EOF
	// 1 - outside the <page> tag
	// 2 - just passed the <page> tag but outside the <revision>
	// 3 - just passed the (next) <revision>
	// 4 - just passed the <timestamp> inside the <revision>
	// 5 - just passed the </timestamp> but still inside the <revision></revision> block
	// 6 - just passed the </revision>
	// 7 - just passed the </page>
	private byte flag;

	// compression mode checking
	private boolean compressed = false;

	// indicating how many <revision> tags have been met, reset after each page end
	private int revisionVisited;

	// indicating the flow condition within [flag = 4]
	// -1 - Unmatched
	//  1 - Matched <revision> tag partially
	//  2 - Matched </page> tag partially
	//  3 - Matched both <revision> and </page> partially
	private int lastMatchTag = -1;

	// a direct buffer to improve the local IO performance
	private byte[] buf = new byte[134217728];
	private int[] pos = new int[2];
	
	private Seekable fsin;

	private DataOutputBuffer pageHeader = new DataOutputBuffer();
	private DataOutputBuffer rev1Buf = new DataOutputBuffer();
	private DataOutputBuffer rev2Buf = new DataOutputBuffer();
	private DataOutputBuffer tsBuf = new DataOutputBuffer();
	
	// remember the last time point
	private DateTime curTs;

	// TODO implement this later
	private Pattern exclude;
	
	// remember the time scale constant
	private TimeScale timeScale;
	
	// mark whether the current revision should be skipped
	private boolean skipped;
	
	private static final DateTimeFormatter dtf = ISODateTimeFormat.dateTimeNoMillis();

	private final LongWritable key = new LongWritable();
	private final Text value = new Text();

	public WikiRevisionDiffReader() {
		super();
	}
	
	public WikiRevisionDiffReader(TimeScale ts) {
		super();
		this.timeScale = ts;
	}
	
	@Override
	public void initialize(InputSplit input, TaskAttemptContext tac)
			throws IOException, InterruptedException {

		Configuration conf = tac.getConfiguration();

		FileSplit split = (FileSplit) input;
		start = split.getStart();
		end = start + split.getLength();
		Path file = split.getPath();

		CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(conf);
		CompressionCodec codec = compressionCodecs.getCodec(file);

		FileSystem fs = file.getFileSystem(conf);

		if (codec != null) { // file is compressed
			compressed = true;
			// fsin = new FSDataInputStream(codec.createInputStream(fs.open(file)));
			CompressionInputStream cis = codec.createInputStream(fs.open(file));

			// This is extremely slow because of I/O overhead
			while (cis.getPos() < start) cis.read();
			fsin = cis;
		} else { // file is uncompressed	
			compressed = false;
			fsin = fs.open(file);
			fsin.seek(start);
		}

		flag = 1;
		revisionVisited = 0;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (fsin.getPos() < end) {
			while (readUntilMatch()) {  
				if (flag == 7) {								
					pageHeader.reset();
					rev1Buf.reset();
					rev2Buf.reset();
					value.clear();
					revisionVisited = 0;	
					skipped = false;
					curTs = null;
				} 
				else if (flag == 6) {
					if (!skipped) {
						value.set(pageHeader.getData(), 0, pageHeader.getLength() 
								- START_REVISION.length);
						value.append(rev1Buf.getData(), 0, rev1Buf.getLength());
						value.append(rev2Buf.getData(), 0, rev2Buf.getLength());
						value.append(END_PAGE, 0, END_PAGE.length);
						
						rev1Buf.reset();
						rev1Buf.write(rev2Buf.getData());
						rev2Buf.reset();
						
						return true;
					}
					skipped = false;
				}
				else if (flag == 2) {
					pageHeader.write(START_PAGE);
				}
				else if (flag == 3) {
					// This is a trick: We mark the offset not to the beginning <revision>,
					// but <timestamp>
					// key.set(fsin.getPos() - START_REVISION.length);
					if (revisionVisited == 0) {							
						rev1Buf.write(DUMMY_REV);
					} 
					rev2Buf.write(START_REVISION);
				}
				else if (flag == 4) {
					tsBuf.reset();
				}
				else if (flag == 5) {
					skipped = shouldSkip();
				}
				else if (flag == -1) {
					pageHeader.reset();
				}
			}
		}
		return false;
	}
	
	// Check whether the current revision should be skipped
	private boolean shouldSkip() {
		if (tsBuf.getLength() == 0)
			return false;
		
		// TODO: Might need to control the exception here
		DateTime tmpDt = dtf.parseDateTime(new String(tsBuf.getData(), 0, tsBuf.getLength() 
				- END_TIMESTAMP.length));
		
		if (curTs == null) {
			curTs = tmpDt;
			return false;
		}
		
		
		// TODO
		return false;
	}
	
	private static MutableDateTime roundup(String timestamp) {
		return null;
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
	InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
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

	private boolean readUntilMatch() throws IOException {

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
				if (flag == 1 || flag == 5) {
					if (b == START_PAGE[i]) {
						i++;
						if (i >= START_PAGE.length) {
							flag = 2;
							return true;
						}
					} else i = 0;
				}

				// put everything between <page> tag and the first <revision> tag into pageHeader
				else if (flag == 2) {
					if (b == START_REVISION[i]) {
						i++;
					} else i = 0;
					pageHeader.write(b);
					if (i >= START_REVISION.length) {
						flag = 3;
						return true;
					}
				}

				// inside <revision></revision> block
				else if (flag == 3) {
					if (b == START_TIMESTAMP[i]) {
						i++;
					} else i = 0;
					rev2Buf.write(b);
					if (i >= START_TIMESTAMP.length) {
						flag = 4;
						// tsBuf.reset();
						return true;
					}
				}

				else if (flag == 4) {
					if (b == END_TIMESTAMP[i]) {
						i++;
					} else i = 0;
					tsBuf.write(b);
					rev2Buf.write(b);
					if (i >= END_TIMESTAMP.length) {
						flag = 5;
						return true;
					}
				}
				
				else if (flag == 5) {
					if (b == END_REVISION[i]) {
						i++;
					} else i = 0;
					if (!skipped) rev2Buf.write(b);
					if (i >= END_REVISION.length) {
						flag = 6;
						//TODO: cho vao nextKeyVAlu
						revisionVisited++;
						return true;
					}
				}
				
				// Note that flag 4 can be the signal of a new record inside one old page
				else if (flag == 6) {
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
						flag = 7;
						lastMatchTag = -1;
						return true;							
					} else if ((lastMatchTag == 1 || lastMatchTag == 3) && i >= START_REVISION.length) {
						flag = 3;
						lastMatchTag = -1;
						return true;
					}				
				} 
			}
		}
	}
}