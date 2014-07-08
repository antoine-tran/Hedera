package org.hedera.mapreduce;

import static org.hedera.io.input.WikiRevisionInputFormat.REVISION_BEGIN_TIME;
import static org.hedera.io.input.WikiRevisionInputFormat.REVISION_END_TIME;
import static org.hedera.io.input.WikiRevisionInputFormat.TIME_FORMAT;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.hedera.io.RevisionHeader;
import org.hedera.io.input.WikiRevisionHeaderInputFormat;
import org.hedera.io.input.WikiRevisionInputFormat;

import tl.lin.data.pair.PairOfIntLong;
import tl.lin.data.pair.PairOfLongs;
import tuan.hadoop.conf.JobConfig;

/** A mini patch that extracts the list of (revisionID, parentID) from the revision sets */
public class SampleRevisionPair extends JobConfig implements Tool {
	private static final Logger LOG = Logger.getLogger(SampleRevisionPair.class);

	private static final class MyMapper extends 
			Mapper<LongWritable, RevisionHeader, LongWritable, PairOfLongs> {
		
		private final LongWritable keyOut = new LongWritable();
		private final PairOfLongs val = new PairOfLongs();
		
		@Override
		protected void map(LongWritable key, RevisionHeader value, Context context) 
				throws IOException, InterruptedException {
			
			// Emit timestamp as key, (revision, parent) as value
			keyOut.set(value.getTimestamp());
			val.set(value.getRevisionId(), value.getParentId());
			context.write(keyOut, val);
		}
	}
	
	private static final class MyReducer extends Reducer<LongWritable, PairOfLongs, 
			LongWritable, Text> {

		private final Text val = new Text();
		
		@Override
		protected void reduce(LongWritable k, Iterable<PairOfLongs> v, Context c)
				throws IOException, InterruptedException {

			for (PairOfLongs pair : v) {
				String text = pair.getLeftElement() + "\t" + pair.getRightElement();
				val.set(text);
				c.write(k,val);				
			}			
		}
	}
	
	public static final String INPUT_OPTION = "input";
	public static final String OUTPUT_OPTION = "output";
	public static final String SEED_OPTION = "seedfile";
	private static final String REDUCE_OPTION = "reduceNo";

	// All begin and end time are in ISOTimeFormat
	private static final String BEGIN_TIME_OPTION = "begin";
	private static final String END_TIME_OPTION = "end";
	
	@SuppressWarnings("static-access")
	@Override
	public int run(String[] args) throws Exception {
		
		Options options = new Options();

		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("input path").create(INPUT_OPTION));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("output path").create(OUTPUT_OPTION));
		options.addOption(OptionBuilder.withArgName("num").hasArg()
				.withDescription("begin time").create(BEGIN_TIME_OPTION));
		options.addOption(OptionBuilder.withArgName("num").hasArg()
				.withDescription("end time").create(END_TIME_OPTION));
		options.addOption(OptionBuilder.withArgName("num").hasArg()
				.withDescription("end time").create(REDUCE_OPTION));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("seed file").create(SEED_OPTION));


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

		if (!cmdline.hasOption(INPUT_OPTION) || !cmdline.hasOption(OUTPUT_OPTION)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(this.getClass().getName(), options);
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}

		String input = cmdline.getOptionValue(INPUT_OPTION);
		String output = cmdline.getOptionValue(OUTPUT_OPTION);
		int reduceNo = 1;

		if (cmdline.hasOption(REDUCE_OPTION)) {
			String reduceNoStr = cmdline.getOptionValue(REDUCE_OPTION);
			try {
				reduceNo = Integer.parseInt(reduceNoStr);
			} catch (NumberFormatException e) {
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp(this.getClass().getName(), options);
				ToolRunner.printGenericCommandUsage(System.out);
				System.err.println("Invalid reduce No. : " + reduceNoStr);
			}
		}
		
		String seedPath = null;
		if (cmdline.hasOption(SEED_OPTION)) {
			seedPath = cmdline.getOptionValue(SEED_OPTION);
		}

		long begin = 0, end = Long.MAX_VALUE;
		if (cmdline.hasOption(BEGIN_TIME_OPTION)) {
			String beginTs = cmdline.getOptionValue(BEGIN_TIME_OPTION);
			try {
				begin = TIME_FORMAT.parseMillis(beginTs);
			} catch (Exception e) {
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp(this.getClass().getName(), options);
				ToolRunner.printGenericCommandUsage(System.out);
				System.err.println("Invalid time format: " + e.getMessage());
			}
		}		

		if (cmdline.hasOption(END_TIME_OPTION)) {
			String endTs = cmdline.getOptionValue(END_TIME_OPTION);
			try {
				end = TIME_FORMAT.parseMillis(endTs);
			} catch (Exception e) {
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp(this.getClass().getName(), options);
				ToolRunner.printGenericCommandUsage(System.out);
				System.err.println("Invalid time format: " + e.getMessage());
			}
		}

		LOG.info("Tool name: " + BasicComputeTermStats.class.getSimpleName());
		LOG.info(" - input: " + input);
		LOG.info(" - output: " + output);		

		// skip non-article
		getConf().setBoolean(WikiRevisionInputFormat.SKIP_NON_ARTICLES, true);

		// set up range
		getConf().setLong(REVISION_BEGIN_TIME, begin);
		getConf().setLong(REVISION_END_TIME, end);
		
		// set up seeds
		if (seedPath != null) getConf().set(WikiRevisionInputFormat.SEED_FILE, seedPath);

		Job job = create(BasicComputeTermStats.class.getSimpleName() + ":" + input,
				BasicComputeTermStats.class);

	    job.setNumReduceTasks(reduceNo);

	    FileInputFormat.setInputPaths(job, input);
	    FileOutputFormat.setOutputPath(job, new Path(output));

	    job.setInputFormatClass(WikiRevisionHeaderInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);

	    job.setMapOutputKeyClass(LongWritable.class);
	    job.setMapOutputValueClass(PairOfIntLong.class);
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(Text.class);

	    job.setMapperClass(MyMapper.class);
	    job.setReducerClass(MyReducer.class);

		FileSystem.get(getConf()).delete(new Path(output), true);

		long startTime = System.currentTimeMillis();
		job.waitForCompletion(true);
		LOG.info("Job Finished in " + (System.currentTimeMillis() - 
				startTime) / 1000.0 + " seconds.");	
		return 0;
	}
	
	public static void main(String[] args) {
		try {
			ToolRunner.run(new SampleRevisionPair(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
