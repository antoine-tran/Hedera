/**
 * This is inspired by the original version of Yusuke Matsubara's StreamWikiDumpInputFormat
 * in wikihadoop project, using the newer Hadoop Mapreduce API. I therefore copied over
 * here all comments, copyright notes etc.
 * 
 * Copyright 2011-2014 Yusuke Matsubara
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.wikimedia.hadoop.io;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
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
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.log4j.Logger;

/** A InputFormat implementation that splits a Wikipedia Revision File into page fragments, output 
 * them as input records.
 *
 * @author Tuan
 * @since 13.04.2014
 *
 */
public class WikipediaRevisionInputFormat extends TextInputFormat {
	public static final String START_TAG_KEY = "xmlinput.start";
	public static final String END_TAG_KEY = "xmlinput.end";

	public static final String RECORD_READER = "wiki.revision.recordreader";

	private static final String START_PAGE_TAG = "<page>";
	private static final String END_PAGE_TAG = "</page>";
	private static final byte[] START_PAGE = START_PAGE_TAG.getBytes(StandardCharsets.UTF_8);
	private static final byte[] END_PAGE = END_PAGE_TAG.getBytes(StandardCharsets.UTF_8);
	private static final byte[] START_REVISION = "<revision>".getBytes(StandardCharsets.UTF_8);
	private static final byte[] END_REVISION = "</revision>".getBytes(StandardCharsets.UTF_8);

	private static long DEFAULT_MAX_BLOCK_SIZE = 134217728l;
	private static long THRESHOLD = 137438953472l;

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, 
			TaskAttemptContext context) {
		Configuration conf = context.getConfiguration();

		// Tu should have done this already (??): Set maximum splitsize to be 128MB
		conf.setLong("mapreduce.input.fileinputformat.split.maxsize", DEFAULT_MAX_BLOCK_SIZE);

		String recordReader = conf.get(RECORD_READER);
		if (recordReader == null || recordReader.equalsIgnoreCase("RevisionPairRecordReader")) {
			return new RevisionPairRecordReader();
		} else if (recordReader.equalsIgnoreCase("RevisionRecordReader")) {
			return new RevisionRecordReader();
		} else return null;
	}

	/*@Override
	public boolean isSplitable(JobContext context, Path path) {
		return false;
	}*/
	
	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException {
		List<InputSplit> splits = new ArrayList<>();
		for (FileStatus fs : listStatus(context)) {
			Path f = fs.getPath();
			Configuration conf = context.getConfiguration();
			FileSystem fsys = f.getFileSystem(conf);
			if (isSplitable(context, fs.getPath())) {
				splits.addAll(getSplitsForXMLTags(fs, conf, THRESHOLD));
			} else {
				long length = fs.getLen();
				BlockLocation[] blkLocs = fsys.getFileBlockLocations(fs, 0, length);
				if (length != 0) {
					splits.add(new FileSplit(f, 0, length, blkLocs[0].getHosts()));
				} else {
					splits.add(new FileSplit(f, 0, length, new String[]{}));
				}
			}
		}
		return splits;
	}

	// Splits a (possibly compressed) xml files by <page></page> chunks, with respect to the 
	// maximum size of one file
	protected List<FileSplit> getSplitsForXMLTags(FileStatus status, Configuration conf, 
			long threshold) throws IOException {

		List<FileSplit> splits = new ArrayList<>();
		Path file = status.getPath();
		if (status.isDirectory()) {
			throw new IOException("Not a file: " + file);
		}

		CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(conf);
		CompressionCodec codec = compressionCodecs.getCodec(file);
		FileSystem fs = file.getFileSystem(conf);
		InputStream fsin = null;

		try {
			if (codec != null) { // file is compressed
				fsin = codec.createInputStream(fs.open(file));
			} else { // file is uncompressed	
				FSDataInputStream stream = fs.open(file);
				stream.seek(0);
				fsin = stream;
			}

			long start = -1l;
			long[] pos = new long[3];
			byte[] buf = new byte[134217728];
			while (true) {
				if (readUntilMatch(fsin, START_PAGE, pos, buf)) {
					if (start < 0)
						start = pos[0] - START_PAGE.length;
					if (readUntilMatch(fsin, END_PAGE, pos, buf)) {
						if (pos[0] - start >= threshold) {
							BlockLocation[] blkLocs = fs.getFileBlockLocations(status, start, 
									pos[0] - start);
							int blkIndex = getBlockIndex(blkLocs, start);
							splits.add(new FileSplit(file, start, pos[0] - start, 
									blkLocs[blkIndex].getHosts()));
							start = -1;
						}
					} else break;
				} else break;
			}
			/*if (start < pos[0]) {
				splits.add(new FileSplit(file, start, pos[0] - start, new String[]{}));
			}*/
		} finally {
			if (fsin != null) fsin.close();
		}
		return splits;
	}
	
	private static boolean readUntilMatch(InputStream in, byte[] match, long[] pos, byte[] buf) 
			throws IOException {
		int i = 0;
		while (true) {
			if (pos[1] == pos[2]) {
				pos[2] = in.read(buf);
				pos[1] = 0;
				if (pos[2] == -1) {
					return false;
				}
			} 
			while (pos[1] < pos[2]) {
				byte b = buf[(int) pos[1]];
				pos[0]++;
				pos[1]++;
				if (b == match[i]) {
					i++;
					if (i >= match.length) {
						return true;
					}
				} else i = 0;
			}
		}
	}

	/** read a meta-history xml file and output as a record every pair of consecutive revisions.
	 * For example,  Given the following input containing two pages and four revisions,
	 * <pre><code>
	 *  &lt;page&gt;
	 *    &lt;title&gt;ABC&lt;/title&gt;
	 *    &lt;id&gt;123&lt;/id&gt;
	 *    &lt;revision&gt;
	 *      &lt;id&gt;100&lt;/id&gt;
	 *      ....
	 *    &lt;/revision&gt;
	 *    &lt;revision&gt;
	 *      &lt;id&gt;200&lt;/id&gt;
	 *      ....
	 *    &lt;/revision&gt;
	 *    &lt;revision&gt;
	 *      &lt;id&gt;300&lt;/id&gt;
	 *      ....
	 *    &lt;/revision&gt;
	 *  &lt;/page&gt;
	 *  &lt;page&gt;
	 *    &lt;title&gt;DEF&lt;/title&gt;
	 *    &lt;id&gt;456&lt;/id&gt;
	 *    &lt;revision&gt;
	 *      &lt;id&gt;400&lt;/id&gt;
	 *      ....
	 *    &lt;/revision&gt;
	 *  &lt;/page&gt;
	 * </code></pre>
	 * it will produce four keys like this:
	 * <pre><code>
	 *  &lt;page&gt;
	 *    &lt;title&gt;ABC&lt;/title&gt;
	 *    &lt;id&gt;123&lt;/id&gt;
	 *    &lt;revision beginningofpage="true"&gt;&lt;text xml:space="preserve"&gt;
	 *    &lt;/text&gt;&lt;/revision&gt;&lt;revision&gt;
	 *      &lt;id&gt;100&lt;/id&gt;
	 *      ....
	 *    &lt;/revision&gt;
	 *  &lt;/page&gt;
	 * </code></pre>
	 * <pre><code>
	 *  &lt;page&gt;
	 *    &lt;title&gt;ABC&lt;/title&gt;
	 *    &lt;id&gt;123&lt;/id&gt;
	 *    &lt;revision&gt;
	 *      &lt;id&gt;100&lt;/id&gt;
	 *      ....
	 *    &lt;/revision&gt;
	 *    &lt;revision&gt;
	 *      &lt;id&gt;200&lt;/id&gt;
	 *      ....
	 *    &lt;/revision&gt;
	 *  &lt;/page&gt;
	 * </code></pre>
	 * <pre><code>
	 *  &lt;page&gt;
	 *    &lt;title&gt;ABC&lt;/title&gt;
	 *    &lt;id&gt;123&lt;/id&gt;
	 *    &lt;revision&gt;
	 *      &lt;id&gt;200&lt;/id&gt;
	 *      ....
	 *    &lt;/revision&gt;
	 *    &lt;revision&gt;
	 *      &lt;id&gt;300&lt;/id&gt;
	 *      ....
	 *    &lt;/revision&gt;
	 *  &lt;/page&gt;
	 * </code></pre>
	 * <pre><code>
	 *  &lt;page&gt;
	 *    &lt;title&gt;DEF&lt;/title&gt;
	 *    &lt;id&gt;456&lt;/id&gt;
	 *    &lt;revision&gt;&lt;revision beginningofpage="true"&gt;&lt;text xml:space="preserve"&gt;
	 *    &lt;/text&gt;&lt;/revision&gt;&lt;revision&gt;
	 *      &lt;id&gt;400&lt;/id&gt;
	 *      ....
	 *    &lt;/revision&gt;
	 *  &lt;/page&gt;
	 * </code></pre> */
	public static class RevisionPairRecordReader extends RecordReader<LongWritable, Text> {
		private static final Logger LOG = Logger.getLogger(RevisionPairRecordReader.class); 		

		private static final byte[] DUMMY_REV = ("<revision beginningofpage=\"true\">"
				+ "<text xml:space=\"preserve\"></text></revision>\n")
				.getBytes(StandardCharsets.UTF_8);

		private long start;
		private long end;

		// A flag that tells in which block the cursor is:
		// -1: EOF
		// 1 - outside the <page> tag
		// 2 - just passed the <page> tag but outside the <revision>
		// 3 - just passed the (next) <revision>
		// 4 - just passed the </revision>
		// 5 - just passed the </page>
		private byte flag;

		// compression mode checking
		private boolean compressed = false;

		// indicating how many <revision> tags have been met, reset after every record
		private int revisionVisited;

		// indicating the flow conditifion within [flag = 4]
		// -1 - Unmatched
		//  1 - Matched <revision> tag partially
		//  2 - Matched </page> tag partially
		//  3 - Matched both <revision> and </page> partially
		private int lastMatchTag = -1;

		private Seekable fsin;

		private DataOutputBuffer pageHeader = new DataOutputBuffer();
		private DataOutputBuffer rev1Buf = new DataOutputBuffer();
		private DataOutputBuffer rev2Buf = new DataOutputBuffer();

		// TODO implement this later
		private Pattern exclude;

		private final LongWritable key = new LongWritable();
		private final Text value = new Text();

		@Override
		public void initialize(InputSplit input, TaskAttemptContext tac)
				throws IOException, InterruptedException {

			// config xmlinput properties to support bzip2 splitting
			Configuration conf = tac.getConfiguration();
			conf.set(START_TAG_KEY, START_PAGE_TAG);
			conf.set(END_TAG_KEY, END_PAGE_TAG);

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
					if (flag == 5) {								
						pageHeader.reset();
						rev1Buf.reset();
						rev2Buf.reset();
						value.clear();
						revisionVisited = 0;						
					} 
					else if (flag == 4) {
						value.set(pageHeader.getData(), 0, pageHeader.getLength() 
								- START_REVISION.length);
						value.append(rev1Buf.getData(), 0, rev1Buf.getLength());
						value.append(rev2Buf.getData(), 0, rev2Buf.getLength());
						value.append(END_PAGE, 0, END_PAGE.length);
						return true;
					}
					else if (flag == 2) {
						pageHeader.write(START_PAGE);
					}
					else if (flag == 3) {
						key.set(fsin.getPos() - START_REVISION.length);
						rev1Buf.reset();
						if (revisionVisited == 0) {							
							rev1Buf.write(DUMMY_REV);
						} else {
							rev1Buf.write(rev2Buf.getData());
						}
						rev2Buf.reset();
						rev2Buf.write(START_REVISION);
					}
					else if (flag == -1) {
						pageHeader.reset();
					}
				}
			}
			return false;
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
			int i = 0;
			while (true) {
				int b = (compressed) ? ((CompressionInputStream)fsin).read() :
					((FSDataInputStream)fsin).read();
				if (b == -1) {
					flag = -1;
					return false;
				}

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
					if (b == END_REVISION[i]) {
						i++;
					} else i = 0;
					rev2Buf.write(b);
					if (i >= END_REVISION.length) {
						flag = 4;
						revisionVisited++;
						return true;
					}
				}

				// Note that flag 4 can be the signal of a new record inside one old page
				else if (flag == 4) {
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
						flag = 5;
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

	/** read a meta-history xml file, output as a record the revision together with the page info.
	 *
	 * For example,  Given the following input,
	 * <pre><code>
	 *  &lt;page&gt;
	 *    &lt;title&gt;ABC&lt;/title&gt;
	 *    &lt;id&gt;123&lt;/id&gt;
	 *    &lt;revision&gt;
	 *      &lt;id&gt;100&lt;/id&gt;
	 *      ....
	 *    &lt;/revision&gt;
	 *    &lt;revision&gt;
	 *      &lt;id&gt;200&lt;/id&gt;
	 *      ....
	 *    &lt;/revision&gt;
	 *    &lt;revision&gt;
	 *      &lt;id&gt;300&lt;/id&gt;
	 *      ....
	 *    &lt;/revision&gt;
	 *  &lt;/page&gt;

	 * </code></pre>
	 * it will produce three keys like this:
	 * <pre><code>
	 *  &lt;page&gt;
	 *    &lt;title&gt;ABC&lt;/title&gt;
	 *    &lt;id&gt;123&lt;/id&gt;
	 *    &lt;revision&gt;
	 *      &lt;id&gt;100&lt;/id&gt;
	 *      ....
	 *    &lt;/revision&gt;
	 *  &lt;/page&gt;
	 * </code></pre>
	 * <pre><code>
	 *  &lt;page&gt;
	 *    &lt;title&gt;ABC&lt;/title&gt;
	 *    &lt;id&gt;123&lt;/id&gt;
	 *    &lt;revision&gt;
	 *      &lt;id&gt;200&lt;/id&gt;
	 *      ....
	 *    &lt;/revision&gt;
	 *  &lt;/page&gt;
	 * </code></pre>
	 * <pre><code>
	 *  &lt;page&gt;
	 *    &lt;title&gt;ABC&lt;/title&gt;
	 *    &lt;id&gt;123&lt;/id&gt;
	 *    &lt;revision&gt;
	 *      &lt;id&gt;300&lt;/id&gt;
	 *      ....
	 *    &lt;/revision&gt;
	 *  &lt;/page&gt;
	 * </code></pre>
	 */
	public static class RevisionRecordReader extends RecordReader<LongWritable, Text> {

		private long start;
		private long end;

		// A flag that tells in which block the cursor is:
		// -1: EOF
		// 1 - outside the <page> tag
		// 2 - just passed the <page> tag but outside the <revision>
		// 3 - just passed the (next) <revision>
		// 4 - just passed the </revision>
		// 5 - just passed the </page>
		private byte flag;

		private FSDataInputStream fsin;
		private DataOutputBuffer pageHeader = new DataOutputBuffer();
		private DataOutputBuffer revBuf = new DataOutputBuffer();

		private final LongWritable key = new LongWritable();
		private final Text value = new Text();

		@Override
		public void initialize(InputSplit input, TaskAttemptContext tac)
				throws IOException, InterruptedException {
			// config xmlinput properties to support bzip2 splitting
			Configuration conf = tac.getConfiguration();
			conf.set(START_TAG_KEY, START_PAGE_TAG);
			conf.set(END_TAG_KEY, END_PAGE_TAG);

			// Tu should have done this already (??): Set maximum splitsize to be 64MB
			conf.setLong("mapreduce.input.fileinputformat.split.maxsize", 67108864l);

			FileSplit split = (FileSplit) input;
			start = split.getStart();
			Path file = split.getPath();

			CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(conf);
			CompressionCodec codec = compressionCodecs.getCodec(file);

			FileSystem fs = file.getFileSystem(conf);

			if (codec != null) { // file is compressed
				fsin = new FSDataInputStream(codec.createInputStream(fs.open(file)));
				end = Long.MAX_VALUE;
			} else { // file is uncompressed	
				fsin = fs.open(file);
				fsin.seek(start);
				end = start + split.getLength();
			}
			flag = 1;
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (fsin.getPos() < end) {
				if (readUntilMatch()) {
					if (flag == 2) {
						key.set(fsin.getPos() - START_PAGE.length);	
					}
					try {
						while (readUntilMatch()) {
							if (flag == 5 || flag == 4) {
								try {
									value.set(pageHeader.getData());
									value.append(revBuf.getData(), 0, revBuf.getLength());
									value.append(END_PAGE, 0, END_PAGE.length);
								} finally {
									if (flag == 5) pageHeader.reset();																		
								}
								return true;
							}							
							else if (flag == -1) {
								pageHeader.reset();
								return false;
							}
						}
					} finally {
						revBuf.reset();						
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
		public Text getCurrentValue() throws IOException, InterruptedException {
			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return (fsin.getPos() - start) / (float) (end - start);
		}

		@Override
		public void close() throws IOException {
			fsin.close();
		}

		private boolean readUntilMatch1() throws IOException {
			int i = 0;
			while (true) {				
				int b = fsin.read();
				if (b == -1) {
					flag = -1;
					return false;
				}

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

				// inside <revision></revision> block everything goes to revBuf
				else if (flag == 3) {
					if (b == END_REVISION[i]) {
						i++;
					} else i = 0;
					revBuf.write(b);
					if (i >= END_REVISION.length) {
						flag = 4;
						return true;
					}
				}

				// Note that flag 4 can be the signal of a new record inside one old page
				else if (flag == 4) {
					if (b == END_PAGE[i]) {
						i++;
						if (i >= END_PAGE.length) {
							flag = 5;
							return true;							
						}
					} else if (b == START_REVISION[i]) {
						i++;
						if (i >= START_REVISION.length) {
							flag = 3;
						}
					} else i = 0;
				} 
			}			
		}

		private boolean readUntilMatch() throws IOException {
			int i = 0;
			while (true) {				
				int b = fsin.read();
				if (b == -1) {
					flag = -1;
					return false;
				}

				// ignore every character until reaching a new page
				if (flag == 1 || flag == 5) {
					if (b == START_PAGE[i]) {
						i++;
						if (i >= START_PAGE.length) {
							pageHeader.write(START_PAGE);
							flag = 2;
							return true;
						}
					} else i = 0;
				}

				// put everything between <page> tag and the first <revision> tag into pageHeader
				else if (flag == 2) {
					if (b == START_REVISION[i]) {
						i++;
						if (i >= START_REVISION.length) {
							revBuf.write(START_REVISION);							
							flag = 3;
							return true;
						}
					} else i = 0;
					pageHeader.write(b);
				}

				// inside <revision></revision> block everything goes to revBuf
				else if (flag == 3) {
					if (b == END_REVISION[i]) {
						i++;
						if (i >= END_REVISION.length) {
							flag = 4;
							revBuf.write(END_REVISION);							
							return true;
						}
					} else i = 0;
					revBuf.write(b);
				}

				// Note that flag 4 can be the signal of a new record inside one old page
				else if (flag == 4) {
					if (b == END_PAGE[i]) {
						i++;
						if (i >= END_PAGE.length) {
							flag = 5;
							return true;							
						}
					} else if (b == START_REVISION[i]) {
						i++;
						if (i >= START_REVISION.length) {
							flag = 3;
							revBuf.write(START_REVISION);
						}
					} else i = 0;
				} 
			}			
		}
	}
}