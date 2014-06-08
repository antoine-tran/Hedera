package org.hedera.io.etl;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.hedera.io.LinkProfile;
import org.hedera.io.RevisionHeader;
import org.hedera.io.LinkProfile.Link;
import org.hedera.io.input.WikiRevisionInputFormat;

/**
 * The input format that supports ETL reading and extract link structures from
 * each revision on the go 
 */
public class RevisionLinkInputFormat extends 
WikiRevisionInputFormat<LongWritable, LinkProfile> {

	@Override
	public RecordReader<LongWritable, LinkProfile> createRecordReader(
			InputSplit input, TaskAttemptContext context) 
					throws IOException, InterruptedException {
		return new RevisionLinkReader();
	}

	public static class RevisionLinkReader 
	extends DefaultRevisionETLReader<LongWritable, 
	LinkProfile> {

		@Override
		protected LongWritable initializeKey() {		
			return new LongWritable();		
		}

		@Override
		protected void freeKey(LongWritable key) {		
		}

		@Override
		protected void freeValue(LinkProfile value) {
			value.clear();
		}

		@Override
		protected LinkProfile initializeValue() {		
			return new LinkProfile();		
		}

		@Override
		protected ETLExtractor<LongWritable, LinkProfile,
		RevisionHeader> initializeExtractor() {		
			return new LinkExtractor();		
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
				RevisionHeader meta) throws IOException {
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
								meta.setLength(buffer.getLength());
								i = 0;
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
	}

	public static class LinkExtractor implements
	ETLExtractor<LongWritable, LinkProfile, RevisionHeader> {

		private static final Logger LOG = Logger.getLogger(LinkExtractor.class);
		private static final byte[] OPEN_BRACKET = "[[".getBytes(StandardCharsets.UTF_8);
		private static final byte[] CLOSE_BRACKET = "]]".getBytes(StandardCharsets.UTF_8);

		@Override
		public float check(RevisionHeader curMeta, RevisionHeader prevMeta) {		
			if (prevMeta == null || prevMeta.getLength() == 0) return 1f;
			if (curMeta.isMinor()) return 0.0005f;
			return (curMeta.getLength() - prevMeta.getLength()) / prevMeta.getLength();
		}

		@Override
		public void extract(DataOutputBuffer content, RevisionHeader meta,
				LongWritable key, LinkProfile value) {

			// add meta-data		
			key.set(meta.getPageId());

			value.clear();

			value.setNamespace(meta.getNamespace());
			value.setPageId(meta.getPageId());
			value.setPageTitle(meta.getPageTitle());
			value.setParentId(meta.getParentId());
			value.setRevisionId(meta.getRevisionId());
			value.setTimestamp(meta.getRevisionId());

			// add content (here the list of links)	
			DataOutputBuffer linkBuffer = new DataOutputBuffer();
			byte[] bytes = content.getData();
			int len = content.getLength();
			int i = 0;

			// flag = 1: not see [[ or has passed ]] token
			// flag = 2: seen [[ but not ]] yet
			int flag = 1;
			try {
				for (int cursor = 0; cursor < len; cursor++) {
					byte b = bytes[cursor];
					if (flag == 1) {				
						if (b == OPEN_BRACKET[i]) {
							i++;					
						} else i = 0;
						if (i >= OPEN_BRACKET.length) {
							flag = 2;
							i = 0;
						}
					}
					else if (flag == 2) {
						if (b == CLOSE_BRACKET[i]) {
							i++;					
						} else i = 0;
						linkBuffer.write(b);
						if (i >= CLOSE_BRACKET.length) {						
							String linkText = new String(linkBuffer.getData(), 0,
									linkBuffer.getLength() - CLOSE_BRACKET.length,
									StandardCharsets.UTF_8);
							Link l = Link.convert(linkText, false);
							if (l != null) {
								value.addLink(l);
							}
							linkBuffer.reset();
							flag = 1;
							i = 0;					
						}
					}		
				}
			} catch (IOException e) {
				LOG.error("Error extracting link from revision: [" 
						+ value.getPageId() + ", rev: " + value.getRevisionId() + "]");
			}
		}
	}
}
