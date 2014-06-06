/**
 * 
 */
package org.hedera.io.etl;

import static org.hedera.io.input.WikiRevisionInputFormat.END_ID;
import static org.hedera.io.input.WikiRevisionInputFormat.END_REVISION;
import static org.hedera.io.input.WikiRevisionInputFormat.END_TIMESTAMP;
import static org.hedera.io.input.WikiRevisionInputFormat.START_ID;
import static org.hedera.io.input.WikiRevisionInputFormat.START_TIMESTAMP;
import static org.hedera.io.input.WikiRevisionInputFormat.TIME_FORMAT;

import java.io.IOException;

import org.apache.hadoop.io.DataOutputBuffer;
import org.hedera.io.WikipediaRevisionHeader;

import edu.umd.cloud9.io.pair.PairOfLongString;
import edu.umd.cloud9.io.pair.PairOfLongs;

/**
 * A lightweight ETL Reader that reads through Wikipedia Revision and extracts
 * revision ids, time stamp and page id / title. only those!!
 * @author tuan
 *
 */
public class WikiRevisionIdsReader extends
DefaultWikiRevisionETLReader<PairOfLongString, PairOfLongs> {

	@Override
	protected ETLExtractor<PairOfLongString, PairOfLongs, 
	WikipediaRevisionHeader> initializeExtractor() {
		return new WikiRevisionHeaderExtractor();
	}

	@Override
	protected PairOfLongString initializeKey() {
		return new PairOfLongString();
	}

	@Override
	protected void freeKey(PairOfLongString key) {
		key.set(0, null);
	}

	@Override
	protected PairOfLongs initializeValue() {
		return new PairOfLongs();
	}

	@Override
	protected void freeValue(PairOfLongs value) {
		value.set(0, 0);
	}

	@Override
	// -1: EOF
	// 9 - default
	// 10 - just passed the inner <id> tag inside <revision>
	// 11 - just passed the inner </id> tag inside <revision>
	// 12 - just passed the <timestamp>
	// 13 - just passed the </timestamp> tag, skip to the </revision>
	// 14 - just passed the </revision>
	protected Ack readToNextRevision(DataOutputBuffer buffer, 
			WikipediaRevisionHeader meta) throws IOException {
		
		int i = 0;
		int flag = 9;	
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

					// after the inner <id>, check for <timestamp>
					else if (flag == 11) {
						if (b == START_TIMESTAMP[i]) {
							i++;
						} else i = 0;
						if (i >= START_TIMESTAMP.length) {
							flag = 12;
							i = 0;
						}	
					}

					// inside <timestamp></timestamp> block everything goes to
					// timestamp buffer
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
							i = 0;
						}
					}				

					// after the </timestamp>, check for <text>
					else if (flag == 13) {
						if (b == END_REVISION[i]) {
							i++;
						} else i = 0;
						if (i >= END_REVISION.length) {
							// the flag is not anymore useful
							flag = 14;
							return Ack.PASSED_TO_NEXT_TAG;
						}
					}
				}
			}
		}
	}
}
