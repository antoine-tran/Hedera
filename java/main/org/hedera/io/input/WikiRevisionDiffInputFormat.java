package org.hedera.io.input;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.hedera.io.WikipediaDiff;

public class WikiRevisionDiffInputFormat 
		extends WikiRevisionInputFormat<WikipediaDiff> {

	@Override
	public RecordReader<LongWritable, WikipediaDiff> createRecordReader(
			InputSplit input, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new DiffReader();
	}
	
	public static class DiffReader extends RecordReader<LongWritable, WikipediaDiff> {

		private LongWritable keyOut = new LongWritable();
		private WikipediaDiff valOut = new WikipediaDiff();
		
		@Override
		public void close() throws IOException {
		}

		@Override
		public LongWritable getCurrentKey() throws IOException,
				InterruptedException {
			return keyOut;
		}

		@Override
		public WikipediaDiff getCurrentValue() throws IOException,
				InterruptedException {
			return valOut;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public void initialize(InputSplit arg0, TaskAttemptContext arg1)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return false;
		}
		
	}

}
