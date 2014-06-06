package org.hedera.io.etl;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.hedera.io.input.WikiRevisionInputFormat;

import edu.umd.cloud9.io.pair.PairOfLongString;
import edu.umd.cloud9.io.pair.PairOfLongs;

public class WikiRevisionIdsFormat extends 
		WikiRevisionInputFormat<PairOfLongString, PairOfLongs> {
	
	// This job is not expensive, so don't bother set high parallel degree
	@Override
	public boolean isSplitable(JobContext context, Path file) {
		return false;
	}

	@Override
	public RecordReader<PairOfLongString, PairOfLongs> createRecordReader(
			InputSplit input, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new WikiRevisionIdsReader();
	}

}
