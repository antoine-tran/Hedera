package org.hedera.mapreduce;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hedera.io.input.FileNullInputFormat;

import tuan.hadoop.conf.JobConfig;

public class TestFileNullInputFormat extends JobConfig implements Tool {

	public static final String INPUT_OPTION = "input";
	public static final String OUTPUT_OPTION = "output";

	@SuppressWarnings("static-access")
	@Override
	public int run(String[] args) throws Exception {
		Options options = new Options();

		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("input path").create(INPUT_OPTION));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("output path").create(OUTPUT_OPTION));

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

		Job job = create("Hedera: Test FileNullInputFormat: " + input,
				TestFileNullInputFormat.class);

		// add all paths from the input dir
		FileSystem fs = FileSystem.get(getConf());
		FileStatus[] statuses = fs.globStatus(new Path(input));
		for (FileStatus status : statuses) {
			Path p = status.getPath();
			MultipleInputs.addInputPath(job, p, FileNullInputFormat.class);
		}

		// register other job info
		Path outpath = new Path(output);
		FileOutputFormat.setOutputPath(job, outpath);

		// Common configurations
		job.getConfiguration().setBoolean(
				"mapreduce.map.tasks.speculative.execution", false);
		job.getConfiguration().setBoolean(
				"mapreduce.reduce.tasks.speculative.execution", false);


		// Option: Java heap space
		job.getConfiguration().set("mapreduce.child.java.opts", "-Xmx1024m");
		job.getConfiguration().set("mapred.child.java.opts", "-Xmx1024m");
		
		setCompressOption(job);
		job.setNumReduceTasks(1);
		
		// set mapreduce classes
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setInputFormatClass(FileNullInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(Mapper.class);
		job.setReducerClass(Reducer.class);
		
		job.waitForCompletion(true);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new TestFileNullInputFormat(), args);
	}
}
