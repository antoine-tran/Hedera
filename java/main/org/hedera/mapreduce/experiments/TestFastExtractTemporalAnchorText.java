package org.hedera.mapreduce.experiments;

import static org.hedera.io.input.WikiRevisionInputFormat.TIME_FORMAT;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hedera.io.WikipediaLinkSnapshot;
import org.hedera.io.WikipediaLinkSnapshot.Link;
import org.hedera.io.etl.WikiRevisionLinkInputFormat;
import org.hedera.io.input.WikiRevisionInputFormat;
import org.mortbay.log.Log;

import edu.umd.cloud9.io.pair.PairOfLongs;
import tuan.hadoop.conf.JobConfig;

public class TestFastExtractTemporalAnchorText extends JobConfig implements Tool {


	private static final class MyMapper extends Mapper<LongWritable,
	WikipediaLinkSnapshot, PairOfLongs, Text> {

		private PairOfLongs keyOut = new PairOfLongs();
		private Text valOut = new Text();

		// simple counter to sparse the debug printout
		private long cnt;

		@Override
		protected void setup(Context context) throws IOException,
		InterruptedException {
			super.setup(context);
			cnt = 0;
		}

		@Override
		// Output anchor in format (separated by TAB)
		// [timestamp] [source ID] [revision ID] [previous revision ID] [source title] [anchor text] [target title]
		protected void map(LongWritable key, WikipediaLinkSnapshot value,
				Context context) throws IOException, InterruptedException {
			long timestamp = value.getTimestamp();
			String ts = TIME_FORMAT.print(timestamp);
			long pageId = value.getPageId();
			long revId = value.getRevisionId();
			long parId = value.getParentId();

			keyOut.set(revId, timestamp);

			String title = value.getPageTitle();			
			StringBuilder prefix = new StringBuilder();
			prefix.append(ts);
			prefix.append("\t");
			prefix.append(pageId);
			prefix.append("\t");
			prefix.append(revId);
			prefix.append("\t");
			prefix.append(parId);
			prefix.append("\t");
			prefix.append(title);
			prefix.append("\t");
			String s = prefix.toString();
			if (value.getLinks() != null) {
				for (Link link : value.getLinks()) {
					String anchor = link.getAnchorText();
					String target = link.getTarget();
					String output = s + "\t" + anchor + "\t" + target;
					valOut.set(output);

					// debug hook
					cnt++;
					if (cnt % 1000000l == 0)
						Log.info(output);

					context.write(keyOut, valOut);
				}
			}
		}		
	}

	@Override
	public int run(String[] args) throws Exception {
		String inputDir = args[0];
		String outputDir = args[1];
		int reduceNo = Integer.parseInt(args[2]);

		// this job sucks big memory
		setMapperSize("-Xmx5120m");

		Job job = setup("For Avishek: Fast extracting temporal anchor text from "
				+ "Wikipedia revision",
				TestFastExtractTemporalAnchorText.class, inputDir, outputDir,
				WikiRevisionLinkInputFormat.class, TextOutputFormat.class,
				PairOfLongs.class, Text.class, PairOfLongs.class, Text.class,
				MyMapper.class, Reducer.class, reduceNo);

		// skip non-article
		getConf().setBoolean(WikiRevisionInputFormat.SKIP_NON_ARTICLES, true);

		// compress output
		if (args.length >= 4) {			
			setCompress(args[3]);
		}
		
		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new TestFastExtractTemporalAnchorText(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
