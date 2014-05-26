/**
 * 
 */
package org.hedera.io.input;

import java.io.IOException;

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
import static org.hedera.io.input.WikipediaRevisionInputFormat.DEFAULT_MAX_BLOCK_SIZE;
import static org.hedera.io.input.WikipediaRevisionInputFormat.END_ID;
import static org.hedera.io.input.WikipediaRevisionInputFormat.END_PAGE;
import static org.hedera.io.input.WikipediaRevisionInputFormat.END_PAGE_TAG;
import static org.hedera.io.input.WikipediaRevisionInputFormat.END_REVISION;
import static org.hedera.io.input.WikipediaRevisionInputFormat.START_ID;
import static org.hedera.io.input.WikipediaRevisionInputFormat.START_PAGE;
import static org.hedera.io.input.WikipediaRevisionInputFormat.START_PAGE_TAG;
import static org.hedera.io.input.WikipediaRevisionInputFormat.START_REVISION;

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
 * @author tuan
 */
public class WikiRevisionPlainTextReader extends RecordReader<LongWritable, Text> {

	private long start;
	private long end;

	// A flag that tells in which block the cursor is:
	// -1: EOF
	// 1 - outside the <page> tag
	// 2 - just passed the <page> tag but outside the <id>
	// 3 - just passed the <id> tag
	// 4 - just passed the </id> tag but outside the <revision>
	// 5 - just passed the (next) <revision>
	// 6 - just passed the </revision>
	// 7 - just passed the </page>
	
	private byte flag;

	// compression mode checking
	private boolean compressed = false;

	// indicating the flow conditifion within [flag = 6]
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
	
	// We now convert and cache everything from pageHeader to the followin global variables
	// NOTE: they all need to be synchronized with pageHeader !!
	
	private DataOutputBuffer revBuf = new DataOutputBuffer();
	private DataOutputBuffer keyBuf = new DataOutputBuffer();

	private final LongWritable key = new LongWritable();
	private final Text value = new Text();

	@Override
	public void initialize(InputSplit input, TaskAttemptContext tac)
			throws IOException, InterruptedException {

		// config xmlinput properties to support bzip2 splitting
		Configuration conf = tac.getConfiguration();

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
				if (flag == 7) {
					pageHeader.reset();
					value.clear();
				} 
				else if (flag == 6) {					
					value.set(pageHeader.getData(), 0, pageHeader.getLength() 
							- START_REVISION.length);
					
					value.append(revBuf.getData(), 0, revBuf.getLength());
					value.append(END_PAGE, 0, END_PAGE.length);
					revBuf.reset();
					return true;
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
					revBuf.write(START_REVISION);
				}
				else if (flag == -1) {
					pageHeader.reset();
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
		if (compressed) {
			((CompressionInputStream)fsin).close();
		} else {
			((FSDataInputStream)fsin).close();
		}
	}
	
	// Scan the tags in SAX manner. Return at every legit tag and inform the program via the global flag
	// Flush into the caches if necessary
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
				if (flag == 1 || flag == 7) {
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
				
				// inside <revision></revision> block
				else if (flag == 5) {
					if (b == END_REVISION[i]) {
						i++;
					} else i = 0;
					revBuf.write(b);
					if (i >= END_REVISION.length) {
						flag = 6;
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
						flag = 5;
						lastMatchTag = -1;
						return true;
					}				
				} 
			}		
		}
	}
}
