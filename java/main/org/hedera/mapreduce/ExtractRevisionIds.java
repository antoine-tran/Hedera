package org.hedera.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hedera.io.etl.RevisionIdsFormat;
import org.hedera.io.input.WikiRevisionInputFormat;

import edu.umd.cloud9.io.pair.PairOfLongs;
import tuan.hadoop.conf.JobConfig;

public class ExtractRevisionIds extends JobConfig implements Tool {

	private static final class MyReducer extends 
			Reducer<LongWritable, PairOfLongs, LongWritable, Text> {

		private LongWritable keyOut = new LongWritable();
		private Text valOut = new Text();
		
		@Override
		protected void reduce(LongWritable k, Iterable<PairOfLongs> vs,
				Context context) throws IOException, InterruptedException {
			keyOut.set(k.get());
			for (PairOfLongs v : vs) {
				valOut.set(v.getLeftElement() + "\t" + v.getRightElement());
				context.write(keyOut, valOut);
			}				
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		String inputDir = args[1];
		String outputDir = args[2];
		String name = args[0];
		int reduceNo = Integer.parseInt(args[3]);

		setMapperSize("-Xmx2048m");

		// skip non-article
		getConf().setBoolean(WikiRevisionInputFormat.SKIP_NON_ARTICLES, true);

		Job job = setup("Hedera: " + name,
				ExtractRevisionIds.class, inputDir, outputDir,
				RevisionIdsFormat.class, TextOutputFormat.class,
				LongWritable.class, PairOfLongs.class, 
				LongWritable.class, Text.class,
				Mapper.class, MyReducer.class, reduceNo);
		
		
		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new ExtractRevisionIds(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
