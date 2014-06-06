package org.hedera.io.input;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class WikiRevisionPairInputFormat 
		extends WikiRevisionInputFormat<LongWritable, Text> {

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, 
			TaskAttemptContext context) {
		return new RevisionReader();
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
	 * </code></pre> 
	 */ 
	 // State of the flag:
	 // 1 - beginning of the first <page> tag
	 // 2 - just passed the <page> tag but outside the <id> tag
	 // 3 - just passed the <id>
	 // 4 - just passed the </id> but outside <revision>
	 // 5 - just passed the <revision>
	 // 6 - just passed the </revision>
	 // 7 - just passed the </page>	 
	public static class RevisionReader extends WikiRevisionReader<Text> {

		private static final byte[] DUMMY_REV = ("<revision beginningofpage=\"true\">"
				+ "<timestamp>1970-01-01T00:00:00Z</timestamp><text xml:space=\"preserve\">"
				+ "</text></revision>\n")
				.getBytes(StandardCharsets.UTF_8);


		// indicating how many <revision> tags have been met, reset after every page end
		private int revisionVisited;

		// indicating the flow conditifion within [flag = 6]
		// -1 - Unmatched
		//  1 - Matched <revision> tag partially
		//  2 - Matched </page> tag partially
		//  3 - Matched both <revision> and </page> partially
		private int lastMatchTag = -1;

		private DataOutputBuffer pageHeader = new DataOutputBuffer();
		private DataOutputBuffer keyBuf = new DataOutputBuffer();
		private DataOutputBuffer rev1Buf = new DataOutputBuffer();
		private DataOutputBuffer rev2Buf = new DataOutputBuffer();

		@Override
		public void initialize(InputSplit input, TaskAttemptContext tac)
				throws IOException, InterruptedException {
			super.initialize(input, tac);
			revisionVisited = 0;
			value = new Text();
		}

		@Override
		public STATE doWhenMatch() throws IOException, InterruptedException {
			if (flag == 7) {								
				pageHeader.reset();
				rev1Buf.reset();
				rev2Buf.reset();
				value.clear();
				revisionVisited = 0;						
			} 
			else if (flag == 6) {					
				value.set(pageHeader.getData(), 0, pageHeader.getLength() 
						- START_REVISION.length);
				value.append(rev1Buf.getData(), 0, rev1Buf.getLength());
				value.append(rev2Buf.getData(), 0, rev2Buf.getLength());
				value.append(END_PAGE, 0, END_PAGE.length);
				return STATE.STOP_TRUE;
			}
			else if (flag == 4) {
				String pageId = new String(keyBuf.getData(), 0, keyBuf.getLength()
						- END_ID.length);
				key.set(Long.parseLong(pageId));	
				keyBuf.reset();
			}				
			else if (flag == 2) {
				pageHeader.write(START_PAGE);
			}
			else if (flag == 5) {
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
				return STATE.STOP_FALSE;
			}
			return STATE.CONTINUE;
		}

		@Override
		protected boolean readUntilMatch() throws IOException {

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
						rev2Buf.write(b);
						if (i >= END_REVISION.length) {
							flag = 6;
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
