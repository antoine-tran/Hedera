package org.hedera.mapreduce.experiments;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hedera.io.WikipediaRevision;
import org.hedera.io.WikipediaRevisionDiff;
import org.hedera.io.input.WikiRevisionDiffInputFormat;
import org.hedera.io.input.WikiRevisionPageInputFormat;
import org.hedera.io.input.WikiRevisionTextInputFormat;

import tuan.hadoop.conf.JobConfig;

/**
 * Map-Reduce job that tests the WikipediaPageInputFormat
 */
public class TestWikipediaPageInputFormat extends JobConfig implements Tool {

	private static final class MyMapper extends 
	Mapper<LongWritable, WikipediaRevisionDiff, LongWritable, Text> {

		LongWritable key = new LongWritable();
		Text value = new Text();

		private Random r = new Random();

		@Override
		protected void map(LongWritable k, WikipediaRevisionDiff v,
				Context context) throws IOException, InterruptedException {

			double d = r.nextDouble();
			if (d >= 0.67d) {
				key.set(k.get());			
				value.set(v.getPageTitle());
				context.write(key,value);
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = setup("Hedera: Test WikiRevisionPageInputFormat",
				TestWikipediaPageInputFormat.class, 
				args[0], args[1],
				WikiRevisionDiffInputFormat.class, 
				TextOutputFormat.class, 
				LongWritable.class, Text.class,
				LongWritable.class, Text.class,
				MyMapper.class, Reducer.class,
				20);		
		
		// Delete the output directory if it exists already.
		FileSystem.get(job.getConfiguration()).delete(new Path(args[1]), true);
				
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
