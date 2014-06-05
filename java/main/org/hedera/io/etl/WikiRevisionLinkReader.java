package org.hedera.io.etl;

import java.io.IOException;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.hedera.io.WikipediaLinkSnapshot;
import org.hedera.io.WikipediaRevisionHeader;

import static org.hedera.io.input.WikiRevisionInputFormat.START_TEXT;
import static org.hedera.io.input.WikiRevisionInputFormat.START_ID;
import static org.hedera.io.input.WikiRevisionInputFormat.END_ID;
import static org.hedera.io.input.WikiRevisionInputFormat.END_TEXT;
import static org.hedera.io.input.WikiRevisionInputFormat.START_PARENT_ID;
import static org.hedera.io.input.WikiRevisionInputFormat.END_PARENT_ID;
import static org.hedera.io.input.WikiRevisionInputFormat.START_TIMESTAMP;
import static org.hedera.io.input.WikiRevisionInputFormat.END_TIMESTAMP;
import static org.hedera.io.input.WikiRevisionInputFormat.END_REVISION;
import static org.hedera.io.input.WikiRevisionInputFormat.TIME_FORMAT;

/**
 * This ETLReader reads and extracts the links from wiki revision
 * @author tuan
 *
 */
public class WikiRevisionLinkReader extends DefaultWikiRevisionETLReader<LongWritable, 
		WikipediaLinkSnapshot> {

	@Override
	protected LongWritable initializeKey() {		
		return new LongWritable();		
	}
	
	@Override
	protected WikipediaLinkSnapshot initializeValue() {		
		return new WikipediaLinkSnapshot();		
	}
	
	@Override
	// -1: EOF
	// 9 - just passed the (next) <revision>
	// 10 - just passed the inner <id> tag inside <revision>
	// 11 - just passed the inner </id> tag inside <revision>
	// 12 - just passed the <timestamp>
	// 13 - just passed the </timestamp> tag
	// 14 - just passed the <parentId>
	// 15 - just passed the </parentId> tag
	// 16 - just passed the <text> tag
	// 17 - just passed the </text> tag
	// 18 - just passed the </revision>
	// 19 - just passed the </page>
	protected Ack readToNextRevision(DataOutputBuffer buffer, 
			WikipediaRevisionHeader meta) throws IOException {
		int i = 0;
		int flag = 9;	
		int parOrTs = -1;
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
						} else if ((parOrTs == 1 || parOrTs == 3) && i >= START_PARENT_ID.length) {
							flag = 14;
							parOrTs = -1;							
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
							meta.setTimestamp(timestamp);
							timestampBuf.reset();
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
						}
					}
					
					// after the </parentId>, search for <timestamp>
					else if (flag == 15) {
						if (b == START_TIMESTAMP[i]) {
							i++;
						} else i = 0;
						if (i >= START_TIMESTAMP.length) {
							flag = 12;
						}
					}
					
					// After the timestamp, sometimes we can make a quick check to see
					// whether we should  skip this revision
					
					// after the </timestamp>, check for <text>
					else if (flag == 13) {
						if (b == START_TEXT[i]) {
							i++;
						} else i = 0;
						if (i >= START_TEXT.length) {
							flag = 16;
						}
					}
					
					// inside <text></text> block everything goes to content buffer
					else if (flag == 16) {
						if (b == END_TEXT[i]) {
							i++;
						} else i = 0;
						buffer.write(b);
						if (i >= END_TEXT.length) {
							flag = 17;
							meta.setLength(buffer.getLength()	);
						}
					}

					// look for the closing </revision>
					else if (flag == 17) {
						if (b == END_REVISION[i]) {
							i++;
						} else i = 0;
						if (i >= END_REVISION.length) {
							flag = 18;
							return Ack.PASSED_TO_NEXT_TAG;
						}
					}
				}
			}
		}
	}
}
