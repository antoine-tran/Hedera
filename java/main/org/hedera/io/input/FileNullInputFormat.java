package org.hedera.io.input;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * A virtual input format that checks one file and returns its name to 
 * the mapper. For simple map reduce task separated by files 
 */
public class FileNullInputFormat extends FileInputFormat<Text, NullWritable> {

	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		return false;
	}
	
	@Override
	public RecordReader<Text, NullWritable> createRecordReader(InputSplit in,
			TaskAttemptContext tac) throws IOException, InterruptedException {
		return new FileNullRecordReader();
	}

	public static class FileNullRecordReader extends RecordReader<Text, NullWritable> {
		private FileSplit fileSplit;
		private final Text key = new Text();
		private boolean processed = false;

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			this.fileSplit = (FileSplit) split;
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (!processed) {				
				Path file = fileSplit.getPath();
				key.set(file.getName());				
				processed = true;
				return true;
			}
			return false;
		}

		@Override
		public Text getCurrentKey() throws IOException, InterruptedException {
			return key;
		}

		@Override
		public NullWritable getCurrentValue() throws IOException,
		InterruptedException {
			return NullWritable.get();
		}

		@Override
		public float getProgress() throws IOException {
			return processed ? 1.0f : 0.0f;
		}

		@Override
		public void close() throws IOException {
		}
	}
}
