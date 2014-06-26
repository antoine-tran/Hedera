package org.hedera.mapreduce;

/* 
 * Build compacted vectors. Adapting from ClueWeb tool with FullRevision obj
 * 
 * ClueWeb Tools: Hadoop tools for manipulating ClueWeb collections
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.clueweb.data.PForDocVector;
import org.clueweb.data.VByteDocVector;
import org.clueweb.dictionary.DefaultFrequencySortedDictionary;
import org.clueweb.util.AnalyzerFactory;
import org.hedera.io.FullRevision;
import org.hedera.io.input.WikiFullRevisionJsonInputFormat;
import org.hedera.io.input.WikiRevisionInputFormat;
import org.hedera.util.MediaWikiProcessor;

import tl.lin.data.array.IntArrayWritable;
import tl.lin.lucene.AnalyzerUtils;
import tuan.hadoop.conf.JobConfig;
import static org.hedera.io.input.WikiRevisionInputFormat.REVISION_BEGIN_TIME;
import static org.hedera.io.input.WikiRevisionInputFormat.REVISION_END_TIME;
import static org.hedera.io.input.WikiRevisionInputFormat.TIME_FORMAT;

public class BuildPForDocVectors extends JobConfig implements Tool {
	private static final Logger LOG = Logger.getLogger(BuildPForDocVectors.class);

	private static enum Records {
		TOTAL, PAGES, ERRORS, SKIPPED
	};

	private static Analyzer ANALYZER;

	private static final long MAX_DOC_LENGTH = 512l * 1024l * 1024l * 1024l * 1024l; // Skip document if long than this.

	private static class MyMapper extends
	Mapper<LongWritable, FullRevision, LongWritable, IntArrayWritable> {
		private static final LongWritable DOCID = new LongWritable();
		private static final IntArrayWritable DOC = new IntArrayWritable();

		private DefaultFrequencySortedDictionary dictionary;
		
		private MediaWikiProcessor processor;
		private long begin = 0;
		private long end = Long.MAX_VALUE;
		
		@Override
		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			String path = conf.get(DICTIONARY_OPTION);
			dictionary = new DefaultFrequencySortedDictionary(path, fs);

			String analyzerType = conf.get(PREPROCESSING);
			ANALYZER = AnalyzerFactory.getAnalyzer(analyzerType);
			if (ANALYZER == null) {
				LOG.error("Error: proprocessing type not recognized. Abort " + this.getClass().getName());
				System.exit(1);
			}
			processor = new MediaWikiProcessor();
			begin = conf.getLong(REVISION_BEGIN_TIME, 0);
			end = conf.getLong(REVISION_END_TIME, Long.MAX_VALUE);
		}

		@Override
		public void map(LongWritable key, FullRevision doc, Context context)
				throws IOException, InterruptedException {
			context.getCounter(Records.TOTAL).increment(1);

			long revisionId = doc.getRevisionId();
			if (revisionId != 0) {
				DOCID.set(revisionId);

				context.getCounter(Records.PAGES).increment(1);
				try {
					long timestamp = doc.getTimestamp();
					if (timestamp < begin || timestamp >= end) {
						LOG.info("Skipping " + revisionId + " due to invalid timestamp: ["
								+ begin + ", " + end + "]");
						context.getCounter(Records.SKIPPED).increment(1);
						PForDocVector.toIntArrayWritable(DOC, new int[] {}, 0);
						context.write(DOCID, DOC);
						return;
					}
					
					String content = new String(doc.getText(), StandardCharsets.UTF_8);

					// If the document is excessively long, it usually means that something is wrong (e.g., a
					// binary object). Skip so the parsing doesn't choke.
					// As an alternative, we might want to consider putting in a timeout, e.g.,
					// http://stackoverflow.com/questions/2275443/how-to-timeout-a-thread
					if (content.length() > MAX_DOC_LENGTH) {
						LOG.info("Skipping " + revisionId + " due to excessive length: " + content.length());
						context.getCounter(Records.SKIPPED).increment(1);
						PForDocVector.toIntArrayWritable(DOC, new int[] {}, 0);
						context.write(DOCID, DOC);
						return;
					}

					String cleaned = processor.getContent(content);
					List<String> tokens = AnalyzerUtils.parse(ANALYZER, cleaned);

					int len = 0;
					int[] termids = new int[tokens.size()];
					for (String token : tokens) {
						int id = dictionary.getId(token);
						if (id != -1) {
							termids[len] = id;
							len++;
						}
					}

					PForDocVector.toIntArrayWritable(DOC, termids, len);
					context.write(DOCID, DOC);
				} catch (Exception e) {
					// If Jsoup throws any exceptions, catch and move on, but emit empty doc.
					LOG.info("Error caught processing " + revisionId);
					DOC.setArray(new int[] {}); // Clean up possible corrupted data
					context.getCounter(Records.ERRORS).increment(1);
					PForDocVector.toIntArrayWritable(DOC, new int[] {}, 0);
					context.write(DOCID, DOC);
				}
			}
		}
	}

	public static final String INPUT_OPTION = "input";
	public static final String OUTPUT_OPTION = "output";
	public static final String DICTIONARY_OPTION = "dictionary";
	public static final String REDUCERS_OPTION = "reducers";
	public static final String PREPROCESSING = "preprocessing";

	// All begin and end time are in ISOTimeFormat
	private static final String BEGIN_TIME_OPTION = "begin";
	private static final String END_TIME_OPTION = "end";
	/**
	 * Runs this tool.
	 */
	@SuppressWarnings("static-access")
	public int run(String[] args) throws Exception {
		Options options = new Options();

		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("input path").create(INPUT_OPTION));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("output path").create(OUTPUT_OPTION));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("dictionary").create(DICTIONARY_OPTION));
		options.addOption(OptionBuilder.withArgName("num").hasArg()
				.withDescription("number of reducers").create(REDUCERS_OPTION));
		options.addOption(OptionBuilder.withArgName("string " + AnalyzerFactory.getOptions()).hasArg()
				.withDescription("preprocessing").create(PREPROCESSING));
		options.addOption(OptionBuilder.withArgName("num").hasArg()
				.withDescription("begin time").create(BEGIN_TIME_OPTION));
		options.addOption(OptionBuilder.withArgName("num").hasArg()
				.withDescription("end time").create(END_TIME_OPTION));

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

		if (!cmdline.hasOption(INPUT_OPTION) || !cmdline.hasOption(OUTPUT_OPTION)
				|| !cmdline.hasOption(DICTIONARY_OPTION) || !cmdline.hasOption(PREPROCESSING)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(this.getClass().getName(), options);
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}

		String input = cmdline.getOptionValue(INPUT_OPTION);
		String output = cmdline.getOptionValue(OUTPUT_OPTION);
		String dictionary = cmdline.getOptionValue(DICTIONARY_OPTION);
		String preprocessing = cmdline.getOptionValue(PREPROCESSING);

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
		// skip non-article
		getConf().setBoolean(WikiRevisionInputFormat.SKIP_NON_ARTICLES, false);

		// set up range
		getConf().setLong(REVISION_BEGIN_TIME, begin);
		getConf().setLong(REVISION_END_TIME, end);
		
		Job job = create(BuildPForDocVectors.class.getSimpleName() + ":" + input,
				BuildPForDocVectors.class);

		LOG.info("Tool name: " + BuildPForDocVectors.class.getSimpleName());
		LOG.info(" - input: " + input);
		LOG.info(" - output: " + output);
		LOG.info(" - dictionary: " + dictionary);
		LOG.info(" - preprocessing: " + preprocessing);

		if (cmdline.hasOption(REDUCERS_OPTION)) {
			int numReducers = Integer.parseInt(cmdline.getOptionValue(REDUCERS_OPTION));
			LOG.info(" - reducers: " + numReducers);
			job.setNumReduceTasks(numReducers);
		} else {
			job.setNumReduceTasks(0);
		}

		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.getConfiguration().set(DICTIONARY_OPTION, dictionary);
		job.getConfiguration().set(PREPROCESSING, preprocessing);

		job.setInputFormatClass(WikiFullRevisionJsonInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(IntArrayWritable.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(IntArrayWritable.class);

		job.setMapperClass(MyMapper.class);

		FileSystem.get(getConf()).delete(new Path(output), true);

		long startTime = System.currentTimeMillis();
		job.waitForCompletion(true);
		LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the <code>ToolRunner</code>.
	 */
	public static void main(String[] args) throws Exception {
		/*LOG.info("Running " + BuildPForDocVectors.class.getCanonicalName() + " with args "
        		+ Arrays.toString(args));
    	ToolRunner.run(new BuildPForDocVectors(), args);*/
		long a = 512l * 1024l * 1024l * 1024l * 1024l;
		System.out.println(a);
	}
}