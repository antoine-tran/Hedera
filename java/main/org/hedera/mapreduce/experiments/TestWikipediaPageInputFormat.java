package org.hedera.mapreduce.experiments;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hedera.io.WikiRevisionWritable;
import org.hedera.io.input.WikiRevisionPageInputFormat;
import org.hedera.io.input.WikiRevisionTextInputFormat;

import tuan.hadoop.conf.JobConfig;

/**
 * Map-Reduce job that tests the WikipediaPageInputFormat
 */
public class TestWikipediaPageInputFormat extends JobConfig implements Tool {

	private static final class MyMapper extends 
	Mapper<LongWritable, Text, LongWritable, Text> {

		LongWritable key = new LongWritable();
		Text value = new Text();

		private Random r = new Random();

		@Override
		protected void map(LongWritable k, Text v,
				Context context) throws IOException, InterruptedException {

			double d = r.nextDouble();
			if (d >= 0.67d) {
				key.set(k.get());			
				value.set(v);
				context.write(key,value);
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = setup("Hedera: Test WikipediaPageInputFormat",
				TestWikipediaPageInputFormat.class, 
				args[0], args[1],
				WikiRevisionTextInputFormat.class, 
				TextOutputFormat.class, 
				LongWritable.class, Text.class,
				LongWritable.class, Text.class,
				MyMapper.class, Reducer.class,
				20);		
		boolean res = job.waitForCompletion(true);
		return (res) ? 1 : -1;
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new TestWikipediaPageInputFormat(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
