package org.hedera.mapreduce;

import static org.hedera.io.input.WikiRevisionInputFormat.TIME_FORMAT;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.hedera.io.LinkProfile;
import org.hedera.io.LinkProfile.Link;
import org.hedera.io.etl.RevisionLinkInputFormat;
import org.hedera.io.input.WikiRevisionInputFormat;

import tuan.hadoop.conf.JobConfig;

public class FastExtractTemporalAnchorText extends JobConfig implements Tool {

	public static final String INPUT_OPTION = "input";
	public static final String OUTPUT_OPTION = "output";
	public static final String REDUCENO = "reduce";

	private static final Logger LOG = Logger.getLogger(FastExtractTemporalAnchorText.class);

	private static final long MAX_KEY_RANGE = 10000000l;

	private static final class MyMapper extends Mapper<LongWritable,
	LinkProfile, LongWritable, Text> {

		private LongWritable keyOut = new LongWritable();
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
		protected void map(LongWritable key, LinkProfile value,
				Context context) throws IOException, InterruptedException {

			long timestamp = value.getTimestamp();
			String ts = TIME_FORMAT.print(timestamp);
			long pageId = value.getPageId();
			long revId = value.getRevisionId();
			long parId = value.getParentId();

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
					if (cnt % 10000000l == 0)
						LOG.info(output);

					// re-balance the key to avoid bottleneck in reduce phase
					long k = System.currentTimeMillis() / MAX_KEY_RANGE;
					keyOut.set(k);

					context.write(keyOut, valOut);
				}
			}
		}

	}

	private static final class MyReducer extends 
	Reducer<LongWritable, Text, NullWritable, Text> {

		NullWritable k = NullWritable.get();


		@Override
		protected void reduce(LongWritable key, Iterable<Text> vals, Context context)
				throws IOException, InterruptedException {
			for (Text v : vals) {
				context.write(k, v);
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		Options options = new Options();

		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("input path").create(INPUT_OPTION));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("output path").create(OUTPUT_OPTION));
		options.addOption(OptionBuilder.withArgName("num").hasArg()
				.withDescription("number of reducers").create(REDUCENO));

		CommandLine cmdline;
		CommandLineParser parser = new GnuParser();
		try {
			cmdline = parser.parse(options, args);
		} catch (ParseException exp) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(this.getClass().getName(), options);
			ToolRunner.printGenericCommandUsage(System.out);
			System.err.println("Error parsing command line: " + exp.getMessage());
			return -1;
		}

		if (!cmdline.hasOption(INPUT_OPTION) || !cmdline.hasOption(OUTPUT_OPTION) ||
				!cmdline.hasOption(REDUCENO)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(this.getClass().getName(), options);
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}


		String inputDir = cmdline.getOptionValue(INPUT_OPTION);
		String outputDir = cmdline.getOptionValue(OUTPUT_OPTION);
		int reduceNo = Integer.parseInt(cmdline.getOptionValue(REDUCENO));

		// this job sucks big memory
		setMapperSize("-Xmx5120m");

		// skip non-article
		getConf().setBoolean(WikiRevisionInputFormat.SKIP_NON_ARTICLES, true);

		Job job = setup("Hedera: Fast extraction of temporal anchor texts" ,
				FastExtractTemporalAnchorText.class, inputDir, outputDir,
				RevisionLinkInputFormat.class, TextOutputFormat.class,
				LongWritable.class, Text.class,
				NullWritable.class, Text.class,
				MyMapper.class, MyReducer.class, reduceNo);

		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new FastExtractTemporalAnchorText(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
