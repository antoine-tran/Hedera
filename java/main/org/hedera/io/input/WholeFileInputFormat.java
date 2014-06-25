package org.hedera.io.input;

//cc WholeFileInputFormat An InputFormat for reading a whole file as a record
import java.io.IOException;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.*;

/**
 * force mapper to read the whole file as a record. Taken 
 * from Tom White's book "Hadoop: The definitive Guide":
 * <a>https://github.com/tomwhite/hadoop-book/blob/master/
 * ch07/src/main/java/WholeFileInputFormat.java
 * 
 * @author tomwhite
 * </a>
 */
public class WholeFileInputFormat
extends FileInputFormat<NullWritable, BytesWritable> {

	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		return false;
	}

	@Override
	public RecordReader<NullWritable, BytesWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		WholeFileRecordReader reader = new WholeFileRecordReader();
		reader.initialize(split, context);
		return reader;
	}
}
//^^ WholeFileInputFormat