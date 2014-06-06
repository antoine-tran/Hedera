package org.hedera.io.etl;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.hedera.io.WikipediaLinkSnapshot;
import org.hedera.io.input.WikiRevisionInputFormat;

/**
 * The input format that supports ETL reading and extract link structures on the go 
 */
public class WikiRevisionLinkInputFormat extends 
		WikiRevisionInputFormat<WikipediaLinkSnapshot> {
	
	@Override
	public RecordReader<LongWritable, WikipediaLinkSnapshot> createRecordReader(
			InputSplit input, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new WikiRevisionLinkReader();
	}
}
