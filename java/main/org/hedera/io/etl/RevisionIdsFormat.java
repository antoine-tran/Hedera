package org.hedera.io.etl;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.hedera.io.RevisionHeader;
import org.hedera.io.input.WikiRevisionInputFormat;

import edu.umd.cloud9.io.pair.PairOfLongs;

public class RevisionIdsFormat extends 
WikiRevisionInputFormat<LongWritable, PairOfLongs> {

	// This job is not expensive, so don't bother set high parallel degree
	@Override
	public boolean isSplitable(JobContext context, Path file) {
		return false;
	}

	@Override
	public RecordReader<LongWritable, PairOfLongs> createRecordReader(
			InputSplit input, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new RevisionIdsReader();
	}

	/**
	 * A lightweight ETL Reader that reads through Wikipedia Revision and extracts
	 * revision ids, time stamp and page id / title. only those!!
	 * @author tuan
	 *
	 */
	public class RevisionIdsReader extends
	DefaultRevisionETLReader<LongWritable, PairOfLongs> {

		@Override
		protected ETLExtractor<LongWritable, PairOfLongs, 
		RevisionHeader> initializeExtractor() {
			return new IdExtractor();
		}

		@Override
		protected LongWritable initializeKey() {
			return new LongWritable();
		}

		@Override
		protected void freeKey(LongWritable key) {
			key.set(0);
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
				RevisionHeader meta) throws IOException {
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

	public static class IdExtractor implements
	ETLExtractor<LongWritable, PairOfLongs, 
	RevisionHeader> {

		@Override
		// We keep everything for articles
		public float check(RevisionHeader meta1,
				RevisionHeader meta2) {
			return 1f;
		}

		@Override
		public boolean extract(DataOutputBuffer content, RevisionHeader meta,
				LongWritable key, PairOfLongs value) {
			if (meta == null || (meta.getRevisionId() == 0 
					&& meta.getTimestamp() == 0)) {
				return false;
			}
			long revId = meta.getRevisionId();
			long ts = meta.getTimestamp();

			key.set(meta.getPageId());
			value.set(revId, ts);
			
			return true;
		}
	}
}
