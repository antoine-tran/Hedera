package org.hedera.io.etl;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.hedera.io.RevisionBOW;
import org.hedera.io.RevisionHeader;
import org.hedera.io.input.WikiRevisionInputFormat;

/** Input format that transforms a set of revisions within one unit interval
 *  into a bag of words that appear during the interval */
public class RevisionBOWInputFormat extends
		WikiRevisionInputFormat<LongWritable, RevisionBOW> {

	private long unitInterval = 1000 * 60 * 60;
	
	@Override
	public RecordReader<LongWritable, RevisionBOW> createRecordReader(
			InputSplit input, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new RevisionBOWReader();
	}

	public class RevisionBOWReader extends 
			IntervalRevisionETLReader<LongWritable, RevisionBOW> {

		// maintain the word sequence of last visited page
		private List<String> prevRevWords = new LinkedList<>();
		
		// id, timestamp, length
		private long[] prevRev = new long[3];
		
		@Override
		public void initialize(InputSplit input, TaskAttemptContext tac)
				throws IOException, InterruptedException {
			super.initialize(input, tac);
			Configuration conf = tac.getConfiguration();
			String scale = conf.get(SCALE_OPT);
			if (scale != null) {
				if (HOUR_SCALE_OPT.equals(scale)) {
					unitInterval = 1000 * 60 * 60;
				}
				else if (DAY_SCALE_OPT.equals(scale)) {
					unitInterval = 1000 * 60 * 60 * 24;
				}
				else if (WEEK_SCALE_OPT.equals(scale)) {
					unitInterval = 1000 * 60 * 60 * 24 * 7;
				}
				else if (MONTH_SCALE_OPT.equals(scale)) {
					unitInterval = 1000 * 60 * 60 * 24 * 30;
				}
			}
		}
		
		@Override
		protected ETLExtractor<LongWritable, RevisionBOW, 
				RevisionHeader> initializeExtractor() {
			return new RevisionBOWExtractor(prevRevWords, prevRev);
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
		protected RevisionBOW initializeValue() {
			return new RevisionBOW();
		}

		@Override
		protected void freeValue(RevisionBOW value) {
			value.clear();
		}
		
	}
	
	public class RevisionBOWExtractor implements 
			ETLExtractor<LongWritable, RevisionBOW, RevisionHeader> {

		private List<String> prevRevWords;
		private long[] prevRev;
		
		public RevisionBOWExtractor() {
			super();
		}
		
		public RevisionBOWExtractor(List<String> prevRevWords, long[] prevRev) {
			super();
			this.prevRevWords = prevRevWords;
			this.prevRev = prevRev;
		}

		@Override
		public float check(RevisionHeader metaNow, RevisionHeader metaBefore) {
			if (metaBefore == null || metaBefore.getLength() == 0) return 1f;
			long tsNow = metaNow.getTimestamp();

			if (tsNow - prevRev[1] <= unitInterval) {
				return 0.0005f;
			}
			return (metaNow.getLength() - prevRev[2]) 	
					/ metaBefore.getLength();	
		}

		@Override
		public void extract(DataOutputBuffer content, RevisionHeader meta,
				LongWritable key, RevisionBOW value) {
			// TODO Auto-generated method stub
			
		}
	}
}
