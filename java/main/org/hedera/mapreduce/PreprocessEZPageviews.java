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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import tl.lin.data.array.ArrayListOfIntsWritable;
import tuan.hadoop.conf.JobConfig;

/**
 * This program filters from pageviews EZ dumps
 * (http://dumps.wikimedia.org/other/pagecounts-ez) and extracts only English
 * Wikipedia articles. Then it converts all time series into a normal time
 * series in daily scales. Days without views are represented by zeros
 * 
 * The current version works month-wise.
 * 
 * TODO: Add hour-scale time series conversion option
 */
public class PreprocessEZPageviews extends JobConfig implements Tool {

	private static final Logger LOG = Logger
			.getLogger(PreprocessEZPageviews.class);

	public static final String BEGIN_TIME_OPT = "begin";
	public static final String END_TIME_OPT = "end";
	public static final String HDFS_BEGIN_TIME = "wikipedia.begin.time";
	public static final String HDFS_END_TIME = "wikipedia.end.time";

	private static final String DAY_OF_MONTH = "day.Of.Month";
	private static final String MONTH_AS_INT = "month.As.Int";

	private static final DateTimeFormatter dtfMonth = DateTimeFormat
			.forPattern("YYYY-mm");
	private static final DateTimeFormatter dtfMonthPrinter = DateTimeFormat
			.forPattern("YYYYmm");

	@SuppressWarnings("static-access")
	@Override
	/**
	 * Extra options: Seed file path
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

	private static final class MyMapper extends
	Mapper<LongWritable, Text, Text, ArrayListOfIntsWritable> {
		private Text key = new Text();
		private ArrayListOfIntsWritable value = null;

		public void setup(Context context) throws IOException,
		InterruptedException {

			Configuration conf = context.getConfiguration();
			int daysOfMonth = conf.getInt(DAY_OF_MONTH, 30);
			int monthAsInt = conf.getInt(MONTH_AS_INT, 0);

			// initialize the count value
			// value = new ArrayListOfIntsWritable(new int[daysOfMonth + 2]);

			// because of the stupid way of constructing fixed-sized array in ArrayListsOfInts, we
			// must explicitly specify the size in one extra step - and lose a portion of memories
			// for the array tails ...
			value = new ArrayListOfIntsWritable((daysOfMonth + 2) * 3 / 2 + 2);
			value.setSize(daysOfMonth + 2);
			value.set(0, monthAsInt);
		}

		@Override
		protected void map(LongWritable keyIn, Text valueIn, Context context)
				throws IOException, InterruptedException {

			// Skip comments
			String line = valueIn.toString();
			if ((line.charAt(0) != 'e' && line.charAt(0) != 'E')
					|| (line.charAt(1) != 'n' && line.charAt(0) != 'E')
					|| line.charAt(2) != '.'
					|| (line.charAt(3) != 'z' && line.charAt(0) != 'Z')) {
				return;
			}
			int i = line.indexOf(' ');
			int j = line.indexOf(' ', i + 1);

			/*
			 * =================================================================
			 * Process the title
			 * =================================================================
			 */
			String title = line.substring(i + 1, j);
			try {
				title = URLDecoder.decode(title, "UTF-8");
			} catch (Exception e) {
				// LOG.error("Error: " + line, e);
				return;
			}
			if (title.length() > 50)
				return;

			// heuristics:
			// Remove non-articles, Main page, index.html
			if (title.startsWith("Category:") || title.startsWith("File:")
					|| title.startsWith("Wikipedia:")
					|| title.startsWith("Wikipedia/")
					|| title.startsWith("Wikipedia#")
					|| title.startsWith("User:")
					|| title.startsWith("Special:")
					|| title.startsWith("Portal:")
					|| title.startsWith("Portal_talk:")
					|| title.startsWith("Talk:") || title.startsWith("/Talk:")
					|| title.startsWith("Help:")
					|| title.startsWith("Template:")
					|| title.startsWith("Translate:")
					|| title.startsWith("http://")
					|| title.startsWith("https://")
					|| title.startsWith("//upload")
					|| title.startsWith("/File:") || title.endsWith(".html")
					|| title.endsWith("HTML") || title.endsWith(".jpg")
					|| title.endsWith(".txt") || title.endsWith(".TXT")
					|| title.endsWith(".JPG") || title.endsWith(".gif")
					|| title.endsWith(".GIF") || title.endsWith(".css")
					|| title.endsWith(".CSS") || title.endsWith(".bmp")
					|| title.endsWith(".php") || title.endsWith(".BMP")
					|| title.endsWith(".svg") || title.endsWith(".SVG")
					|| title.endsWith(".OGG") || title.endsWith(".ogg")
					|| title.endsWith(".ogv") || title.endsWith(".webm")
					||

					// different language & projects
					title.startsWith("hr:") || title.startsWith("hu:")
					|| title.startsWith("simple:")) {
				return;
			}
			int tmpIdx = 0;

			// heuristics: Normalize titles based on:
			// - Cut off the trailing anchor (following the #), or query string
			// (following the &)
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
			} else if (title.startsWith("''") && title.endsWith("''")) {
				if (title.length() > 2) {
					title = title.substring(2, title.length() - 2);
				}
			} else if (title.startsWith("\"") && title.endsWith("\"")) {
				if (title.length() > 1) {
					title = title.substring(1, title.length() - 1);
				}
			} else if (title.startsWith("wiki/")) {
				title = title.substring(5, title.length());
			}

			title = title.replace(' ', '_');
			key.set(title);

			/*
			 * =================================================================
			 * Process the time series
			 * =================================================================
			 */
			int k = line.indexOf(' ', j + 1);
			int total = Integer.parseInt(line.substring(j + 1, k));

			String compactTs = line.substring(k + 1);

			// reset the time series
			resetTimeseries(value);
			value.set(1, total);

			// decode the time series
			int idx = 0;

			while (idx >= 0) {
				int nextIdx = compactTs.indexOf(',', idx + 1);
				if (nextIdx < 0) {
					break;
				}

				// everything from idx+1 to nextIdx-1 is hourly time series for one day
				extractViewsForOneDay(compactTs, idx + 1, nextIdx);

				idx = nextIdx;
			}	

			context.write(key, value);
		}

		/** return the zero-based index of the day */
		private static int decodeDay(char dayChr) {
			if (dayChr >= 'A' && dayChr <= 'Z') {
				return (dayChr - 'A');
			} else if (dayChr == '[') {
				return 26;
			} else if (dayChr == '\\') {
				return 27;
			} else if (dayChr == ']') {
				return 28;
			} else if (dayChr == '^') {
				return 29;
			} else if (dayChr == '_') {
				return 30;
			} else {
				throw new IllegalArgumentException("Unknown day: " + dayChr);
			}
		}

		/** return the zero-based index of the hour in a day */
		private static int decodeHour(char chr) {
			if (chr >= 'A' && chr <= 'Z') {
				return (chr - 'A');
			} else {
				throw new IllegalArgumentException("Unknown hour: " + chr);
			}
		}

		private void extractViewsForOneDay(CharSequence compactTs, int begin,
				int end) {

			// first character is the day index
			int dayIdx = decodeDay(compactTs.charAt(begin));

			// heuristic, maximum number of views per hour is 999
			int hourIdx = -1;
			int hourView = 0;
			int dayView = 0;

			for (int i = begin + 1; i < end; i++) {
				char chr = compactTs.charAt(i);
				if (chr >= '0' && chr <= '9') {
					hourView = hourView * 10 + (chr - '0');
				} else {
					if (hourIdx >= 0) {

						// TODO: separate the hour processing option here
						dayView += hourView;
					}
					hourIdx = decodeHour(chr);
					hourView = 0;
				}
			}

			// last hour slot
			if (hourIdx >= 0 && hourView > 0) {
				dayView += hourView;
			}

			value.set(dayIdx + 2, dayView);
		}
	}

	private static class MyCombiner extends
	Reducer<Text, ArrayListOfIntsWritable, Text, ArrayListOfIntsWritable> {

		private ArrayListOfIntsWritable value = null;

		public void setup(Context context) throws IOException,
		InterruptedException {

			Configuration conf = context.getConfiguration();
			int daysOfMonth = conf.getInt(DAY_OF_MONTH, 30);
			int monthAsInt = conf.getInt(MONTH_AS_INT, 0);

			// initialize the count value
			// because of the stupid way of constructing fixed-sized array in ArrayListsOfInts, we
			// must explicitly specify the size in one extra step - and lose a portion of memories
			// for the array tails ...
			value = new ArrayListOfIntsWritable((daysOfMonth + 2) * 3 / 2 + 2);
			value.setSize(daysOfMonth + 2);
			value.set(0, monthAsInt);
		}

		@Override
		protected void reduce(Text k, Iterable<ArrayListOfIntsWritable> lists,
				Context context) throws IOException, InterruptedException {

			resetTimeseries(value);

			/* All time series must have the same length */
			for (ArrayListOfIntsWritable ts : lists) {
				for (int i = 1; i < ts.size(); i++) {
					int cnt = ts.get(i);
					cnt += value.get(i);
					value.add(i, cnt);
				}
			}
			context.write(k, value);
		}
	}

	private static class MyReducer extends
	Reducer<Text, ArrayListOfIntsWritable, Text, Text> {

		private ArrayListOfIntsWritable value = null;
		private Text output = new Text();
		private int daysOfMonth;

		public void setup(Context context) throws IOException,
		InterruptedException {

			Configuration conf = context.getConfiguration();
			daysOfMonth = conf.getInt(DAY_OF_MONTH, 30);
			int monthAsInt = conf.getInt(MONTH_AS_INT, 0);

			// initialize the count value
			// because of the stupid way of constructing fixed-sized array in ArrayListsOfInts, we
			// must explicitly specify the size in one extra step - and lose a portion of memories
			// for the array tails ...
			value = new ArrayListOfIntsWritable((daysOfMonth + 2) * 3 / 2 + 2);
			value.setSize(daysOfMonth + 2);
			value.set(0, monthAsInt);
		}

		@Override
		protected void reduce(Text k, Iterable<ArrayListOfIntsWritable> lists,
				Context context) throws IOException, InterruptedException {

			resetTimeseries(value);

			/* All time series must have the same length */
			for (ArrayListOfIntsWritable ts : lists) {
				for (int i = 1; i < ts.size(); i++) {
					int cnt = ts.get(i);
					cnt += value.get(i);
					value.add(i, cnt);
				}
			}

			// 7 characters for the month value, plus the total, which often
			// have 4-5 digits
			StringBuilder sb = new StringBuilder(daysOfMonth * 2 + 12);			
			for (int i = 0; i < value.size(); i++) {
				sb.append(value.get(i));
				sb.append('\t');
			}
			output.set(sb.toString());
			context.write(k, output);
		}
	}

	private static void resetTimeseries(ArrayListOfIntsWritable value) {
		for (int i = 1; i < value.size(); i++) {
			value.set(i, 0);
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		Job job = setup(TextInputFormat.class, TextOutputFormat.class,
				Text.class, ArrayListOfIntsWritable.class, Text.class,
				Text.class, MyMapper.class, MyReducer.class, MyCombiner.class,
				args);

		// Input is a single file of monthly page view
		// cache the seeds
		int i = input.indexOf("pagecounts-");
		int j = input.indexOf("-ge", i + 11);
		if (j < 0)
			return -1;

		DateTime month = dtfMonth.parseDateTime(input.substring(i + 11, j));
		int monthAsInt = Integer.parseInt(dtfMonthPrinter.print(month));
		int daysOfMonth = month.dayOfMonth().getMaximumValue();

		job.getConfiguration().setInt(DAY_OF_MONTH, daysOfMonth);
		job.getConfiguration().setInt(MONTH_AS_INT, monthAsInt);

		job.getConfiguration().set("mapreduce.map.memory.mb", "4096");
		job.getConfiguration().set("mapreduce.reduce.memory.mb", "4096");
		job.getConfiguration().set("mapreduce.map.java.opts", "-Xmx4096m");
		job.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx4096m");
		job.getConfiguration()
		.set("mapreduce.job.user.classpath.first", "true");

		// register the extra options
		if (command.hasOption(BEGIN_TIME_OPT))
			job.getConfiguration().set(HDFS_BEGIN_TIME,
					command.getOptionValue(BEGIN_TIME_OPT));
		if (command.hasOption(END_TIME_OPT))
			job.getConfiguration().set(HDFS_END_TIME,
					command.getOptionValue(END_TIME_OPT));

		try {
			job.waitForCompletion(true);
		} catch (Exception e) {
			LOG.error("Error: ", e);
			e.printStackTrace();
			throw e;
		}
		return 0;
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new PreprocessEZPageviews(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}