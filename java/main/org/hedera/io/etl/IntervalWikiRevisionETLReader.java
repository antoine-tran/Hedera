/**
 * 
 */
package org.hedera.io.etl;

import static org.hedera.io.input.WikiRevisionInputFormat.END_ID;
import static org.hedera.io.input.WikiRevisionInputFormat.END_PARENT_ID;
import static org.hedera.io.input.WikiRevisionInputFormat.END_REVISION;
import static org.hedera.io.input.WikiRevisionInputFormat.END_TEXT;
import static org.hedera.io.input.WikiRevisionInputFormat.END_TIMESTAMP;
import static org.hedera.io.input.WikiRevisionInputFormat.MINOR_TAG;
import static org.hedera.io.input.WikiRevisionInputFormat.START_ID;
import static org.hedera.io.input.WikiRevisionInputFormat.START_PARENT_ID;
import static org.hedera.io.input.WikiRevisionInputFormat.START_TEXT;
import static org.hedera.io.input.WikiRevisionInputFormat.START_TIMESTAMP;
import static org.hedera.io.input.WikiRevisionInputFormat.TIME_FORMAT;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.hedera.io.WikipediaHeader;
import org.hedera.io.WikipediaRevisionHeader;

/**
 * A WikiRevsionETLReader that skips all revisions out of a specific range
 * @author tuan
 *
 */
public abstract class IntervalWikiRevisionETLReader<KEYIN, VALUEIN> extends
		DefaultWikiRevisionETLReader<KEYIN, VALUEIN> {

	public static final String START_TIME_OPT = "org.hedera.io.etl.starttime";
			public static final String END_TIME_OPT = "org.hedera.io.etl.endtime";
	
	private long startTs = Long.MIN_VALUE;
	private long endTs = Long.MAX_VALUE;
	
	@Override
	public void initialize(InputSplit input, TaskAttemptContext tac)
			throws IOException, InterruptedException {
		super.initialize(input, tac);
		Configuration conf = tac.getConfiguration();
		String startTime = conf.get(START_TIME_OPT);
		if (startTime != null) {
			startTs = TIME_FORMAT.parseMillis(startTime);
		}
		String endTime = conf.get(END_TIME_OPT);		
		if (endTime != null) {
			endTs = TIME_FORMAT.parseMillis(endTime);
		}
	}
	
	@Override
	// -1: EOF
	// 9 - default
	// 10 - just passed the inner <id> tag inside <revision>
	// 11 - just passed the inner </id> tag inside <revision>
	// 12 - just passed the <timestamp>
	// 13 - just passed the </timestamp> tag
	// 14 - just passed the <parentId>
	// 15 - just passed the </parentId> tag
	// 16 - just passed the <minor/> (or not)
	// 17 - just passed the <text> tag
	// 18 - just passed the </text> tag
	// 19 - just passed the </revision>
	protected Ack readToNextRevision(DataOutputBuffer buffer, 
			WikipediaRevisionHeader meta) throws IOException {
		int i = 0;
		int flag = 9;	
		int parOrTs = -1;
		int minorOrText = -1;
		try (DataOutputBuffer revIdBuf = new DataOutputBuffer(); 
				DataOutputBuffer timestampBuf = new DataOutputBuffer(); 
				DataOutputBuffer parBuf = new DataOutputBuffer()) {

			while (true) {
				if (!fetchMore()) return Ack.EOF;
				while (hasData()) {
					byte b = nextByte();
					if (flag == 9) {
						if (b == START_ID[i]) {
							i++;
						} else i = 0;
						if (i >= START_ID.length) {
							flag = 10;
							i = 0;
						}
					}
					
					// everything inside the inner <id></id> 
					// block goes to revision buffer
					else if (flag == 10) {
						if (b == END_ID[i]) {
							i++;
						} else i = 0;
						revIdBuf.write(b);
						if (i >= END_ID.length) {
							flag = 11;
							String idStr = new String(revIdBuf.getData(), 0, 
									revIdBuf.getLength() - END_ID.length);
							long revId = Long.parseLong(idStr);
							meta.setRevisionId(revId);
							revIdBuf.reset();
							i = 0;
						}
					}
					
					// after the inner <id>, check for either <timestamp> or <parentId>
					else if (flag == 11) {
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
							flag = 12;
							parOrTs = -1;		
							i = 0;
						} else if ((parOrTs == 1 || parOrTs == 3) && i >= START_PARENT_ID.length) {
							flag = 14;
							parOrTs = -1;
							i = 0;
						}		
					}
					
					// inside <timestamp></timestamp> block everything goes to timestamp buffer
					else if (flag == 12) {
						if (b == END_TIMESTAMP[i]) {
							i++;
						} else i = 0;
						timestampBuf.write(b);
						if (i >= END_TIMESTAMP.length) {
							flag = 13;
							String ts = new String(timestampBuf.getData(), 0, 
									timestampBuf.getLength() 
									- END_TIMESTAMP.length);
							long timestamp = TIME_FORMAT.parseMillis(ts);
							if (timestamp < startTs || timestamp >= endTs) {
								meta.clear();
								return Ack.SKIPPED;
							}							
							meta.setTimestamp(timestamp);
							timestampBuf.reset();
							i = 0;
						}
					}
					
					// inside <parentId></parentId> block everything goes to parentId buffer
					else if (flag == 14) {
						if (b == END_PARENT_ID[i]) {
							i++;
						} else i = 0;
						parBuf.write(b);
						if (i >= END_PARENT_ID.length) {
							flag = 15;
							String parIdStr = new String(parBuf.getData(), 0, parBuf.getLength() 
									- END_PARENT_ID.length);
							long parId = Long.parseLong(parIdStr);
							meta.setParentId(parId);
							parBuf.reset();
							i = 0;
						}
					}
					
					// after the </parentId>, search for <timestamp>
					else if (flag == 15) {
						if (b == START_TIMESTAMP[i]) {
							i++;
						} else i = 0;
						if (i >= START_TIMESTAMP.length) {
							flag = 12;
							i = 0;
						}
					}
					
					// After the timestamp, sometimes we can make a quick check to see
					// whether we should  skip this revision
					
					// after the </timestamp>, check for <minor/>, if they exist
					else if (flag == 13) {
						int curMatch = 0;				
						if ((i < START_TEXT.length && b == START_TEXT[i]) 
								&& (i < MINOR_TAG.length && b == MINOR_TAG[i])) {
							curMatch = 3;
						} else if (i < START_TEXT.length && b == START_TEXT[i]) {
							curMatch = 1;
						} else if (i < MINOR_TAG.length && b == MINOR_TAG[i]) {
							curMatch = 2;
						}				
						if (curMatch > 0 && (i == 0 || minorOrText == 3 || curMatch == minorOrText)) {					
							i++;			
							minorOrText = curMatch;
						} else i = 0;
						if ((minorOrText == 2 || minorOrText == 3) && i >= MINOR_TAG.length) {
							// update the meta
							meta.setMinor(true);
							flag = 16;
							minorOrText = -1;		
							i = 0;
						} else if ((minorOrText == 1 || minorOrText == 3) && i >= START_TEXT.length) {
							flag = 17;
							minorOrText = -1;
							i = 0;
						}	
					}
					
					// after the <minor/>, and search for <text>
					else if (flag == 16) {
						if (b == START_TEXT[i]) {
							i++;
						} else i = 0;
						if (i >= START_TEXT.length) {
							flag = 17;
							i = 0;
						}
					}
					
					// inside <text></text> block everything goes to content buffer
					else if (flag == 17) {
						if (b == END_TEXT[i]) {
							i++;
						} else i = 0;
						buffer.write(b);
						if (i >= END_TEXT.length) {
							flag = 18;							
							// meta.setLength(buffer.getLength());
							i = 0;
							processRevision(buffer, meta);
						}
					}

					// look for the closing </revision>
					else if (flag == 18) {
						if (b == END_REVISION[i]) {
							i++;
						} else i = 0;
						if (i >= END_REVISION.length) {
							// the flag is not anymore useful
							flag = 19;
							return Ack.PASSED_TO_NEXT_TAG;
						}
					}
				}
			}
		}
	}

	/**
	 * This method processes after caching the currently visited revision. It updates
	 * the meta-data to facilitate the extraction of content right afterwards (in
	 * WikiRevisionETLReader's code)
	 * @param buffer
	 * @param meta
	 */
	protected abstract void processRevision(DataOutputBuffer buffer, WikipediaHeader meta);
}
