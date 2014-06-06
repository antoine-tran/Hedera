package org.hedera.mapreduce.experiments;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hedera.io.etl.WikiRevisionIdsFormat;
import org.hedera.io.input.WikiRevisionInputFormat;

import edu.umd.cloud9.io.pair.PairOfLongString;
import edu.umd.cloud9.io.pair.PairOfLongs;
import tuan.hadoop.conf.JobConfig;

public class TestExtractIds extends JobConfig implements Tool {

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
				TestExtractIds.class, inputDir, outputDir,
				WikiRevisionIdsFormat.class, TextOutputFormat.class,
				PairOfLongString.class, PairOfLongs.class, 
				Text.class, Text.class,
				Mapper.class, Reducer.class, reduceNo);

		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new TestExtractIds(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
