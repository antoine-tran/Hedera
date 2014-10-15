package org.hedera.mapreduce;

import java.io.IOException;
import java.net.URLDecoder;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import tuan.hadoop.conf.JobConfig;

/** 
 * This program filters from pageviews EZ dumps (http://dumps.wikimedia.org/other/pagecounts-ez)
 * and extracts only English Wikipedia articles
 */
public class PreprocessEZPageviews extends JobConfig implements Tool {

	private static final Logger LOG = Logger.getLogger(PreprocessEZPageviews.class);

	public static final String BEGIN_TIME_OPT = "begin";
	public static final String END_TIME_OPT = "end";
	public static final String HDFS_BEGIN_TIME = "wikipedia.begin.time";
	public static final String HDFS_END_TIME = "wikipedia.end.time";
	private static final DateTimeFormatter dtfMonth = DateTimeFormat.forPattern("YYYY-mm"); 
	private static final DateTimeFormatter dtfMonthPrinter = DateTimeFormat.forPattern("YYYYmm");
	private static final DateTimeFormatter dtfDate = DateTimeFormat.forPattern("YYYY-mm-DD");

	@SuppressWarnings("static-access")
	@Override
	/**
	 * Extra options: Seed file path, begin time, end time
	 * 
	 */
	public Options options() {
		Options opts = super.options();
		Option beginOpt = OptionBuilder.withArgName("begin").hasArg(true)
				.withDescription("beginning date").create(BEGIN_TIME_OPT);
		Option endOpt = OptionBuilder.withArgName("end").hasArg(true)
				.withDescription("ending date").create(END_TIME_OPT);
		opts.addOption(beginOpt);
		opts.addOption(endOpt);
		return opts;
	}

	private static final class MyMapper 
	extends Mapper<LongWritable, Text, Text, Text> {
		private Text key = new Text();
		private Text value = new Text();
		private DateTime month, begin, end;

		public void setup(Context context) throws IOException, InterruptedException {

			// cache the seeds
			Configuration conf = context.getConfiguration();
			String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
			int i = filename.indexOf("pagecounts-");
			int j = filename.indexOf("-ge",i+11);
			if (j < 0) return;
			month = dtfMonth.parseDateTime(filename.substring(i+11,j));	

			String beginTime = conf.get(HDFS_BEGIN_TIME);	
			if (beginTime != null) {
				begin = dtfDate.parseDateTime(beginTime);
				LOG.info(begin);
			}

			String endTime = conf.get(HDFS_END_TIME);
			if (endTime != null) {
				end = dtfDate.parseDateTime(endTime);
				LOG.info(end);
			}
		}

		@Override
		protected void map(LongWritable keyIn, Text valueIn, Context context)
				throws IOException, InterruptedException {
			String line = valueIn.toString();
			if (line.charAt(0) != 'e' || line.charAt(1) != 'n' 
					|| line.charAt(2) != '.' || line.charAt(3) != 'z') return;
			int i = line.indexOf(' ');
			int j = line.indexOf(' ', i+1);
			String title = line.substring(i+1, j);
			String compactTs = line.substring(j+1);
			try {
				title = URLDecoder.decode(title, "UTF-8");
			} catch (Exception e) {
				LOG.error("Error: " + line, e);
			}
			if (title.length() > 50) return;

			// heuristics:
			// Remove non-articles, Main page, index.html
			if (title.startsWith("Category:") ||
					title.startsWith("File:") ||
					title.startsWith("Wikipedia:") ||
					title.startsWith("Wikipedia/") ||
					title.startsWith("Wikipedia#") ||
					title.startsWith("User:") ||
					title.startsWith("Special:") ||
					title.startsWith("Portal:") ||
					title.startsWith("Portal_talk:") ||
					title.startsWith("Talk:") ||
					title.startsWith("/Talk:") ||
					title.startsWith("Help:") ||
					title.startsWith("Template:") ||
					title.startsWith("Translate:") ||
					title.startsWith("http://") ||
					title.startsWith("https://") ||
					title.startsWith("//upload") ||
					title.startsWith("/File:") ||
					title.endsWith(".html") ||
					title.endsWith("HTML") ||
					title.endsWith(".jpg") ||
					title.endsWith(".txt") ||
					title.endsWith(".TXT") ||
					title.endsWith(".JPG") ||
					title.endsWith(".gif") ||
					title.endsWith(".GIF") ||
					title.endsWith(".css") ||
					title.endsWith(".CSS") ||
					title.endsWith(".bmp") ||
					title.endsWith(".php") ||
					title.endsWith(".BMP") ||
					title.endsWith(".svg") ||
					title.endsWith(".SVG") ||
					title.endsWith(".OGG") ||
					title.endsWith(".ogg") ||
					title.endsWith(".ogv") ||
					title.endsWith(".webm") ||

					// different language & projects
					title.startsWith("hr:") ||
					title.startsWith("hu:") ||
					title.startsWith("simple:")) {
				return;
			}
			int tmpIdx = 0;

			// heuristics: Normalize titles based on:
			// - Cut off the trailing anchor (following the #), or query string (following the &)
			// - Cut off the leading and trailing quotes (double or triple)
			if ((tmpIdx = title.indexOf('#')) > 0) {
				title = title.substring(0, tmpIdx);
			}
			if ((tmpIdx = title.indexOf('&')) > 0) {
				title = title.substring(0, tmpIdx);
			}
			if (title.startsWith("#")) {
				title = title.substring(1, title.length());
			}
			if (title.startsWith("'''") && title.endsWith("'''")) {
				if (title.length() > 3) {
					title = title.substring(3, title.length() - 3);
				}
			}
			else if (title.startsWith("''") && title.endsWith("''")) {
				if (title.length() > 2) {
					title = title.substring(2, title.length() - 2);
				}	
			}
			else if (title.startsWith("\"") && title.endsWith("\"")) {
				if (title.length() > 1) { 
					title = title.substring(1, title.length() - 1);
				}
			}
			else if (title.startsWith("wiki/")) {
				title = title.substring(5, title.length());
			}

			title = title.replace(' ', '_');

			key.set(title);
			value.set(dtfMonthPrinter.print(month) + compactTs);
			context.write(key,value);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		// Extra command line options
		Job job = setup(TextInputFormat.class, TextOutputFormat.class, 
				Text.class, Text.class, 
				Text.class, Text.class, 
				MyMapper.class, Reducer.class, args);

		// register the extra options
		if (command.hasOption(BEGIN_TIME_OPT))
			job.getConfiguration().set(HDFS_BEGIN_TIME, command.getOptionValue(BEGIN_TIME_OPT));
		if (command.hasOption(END_TIME_OPT))
			job.getConfiguration().set(HDFS_END_TIME, command.getOptionValue(END_TIME_OPT));

		try {
			job.waitForCompletion(true);
		} catch (Exception e) {
			LOG.error("Error: ",e);
			e.printStackTrace();
			throw e;
		}
		return 0;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			ToolRunner.run(new PreprocessEZPageviews(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
