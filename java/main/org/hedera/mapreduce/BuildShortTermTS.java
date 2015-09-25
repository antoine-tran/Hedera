package org.hedera.mapreduce;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.procedure.TObjectIntProcedure;
import tl.lin.data.pair.PairOfStrings;
import tuan.hadoop.conf.JobConfig;

/**
 * Build the short-term time series of mutual information between entities
 * Input: src TAB dest TAB timestamp TAB anchor TAB title TAB offset TAB pre TAB post
 * Output: e1 TAB e2 TAB date TAB count 
 * 
 * @author tuan
 */
public class BuildShortTermTS extends JobConfig implements Tool {

	private static final DateTimeFormatter dtf = DateTimeFormat.forPattern("YYYY-MM-dd");

	private static final String PHASE = "phase";
	private static final String PHASE_OPT = "phase.opt";
	
	// This option is only applicable for phase 3
	private static final String BEGIN_TIME = "begin";
	private static final String END_TIME = "end";

	// Step 1: flatten all destinations into one line
	private static class MyMapper extends Mapper<LongWritable, Text, PairOfStrings, Text> {

		private final PairOfStrings KEY = new PairOfStrings();
		private final Text VAL = new Text();		

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			int i = line.indexOf('\t');
			int j = line.indexOf('\t',i+1);
			int k = line.indexOf('\t',j+1);			

			long timestamp = Long.parseLong(line.substring(j+1, k));
			DateTime dt = new DateTime(timestamp);

			KEY.set(line.substring(0, i), dt.toString(dtf));
			VAL.set(line.substring(i+1, j));

			context.write(KEY, VAL);
		}	
	}

	private static class MyReducer extends Reducer<PairOfStrings, Text, PairOfStrings, IntWritable> {

		private final PairOfStrings KEY = new PairOfStrings();
		private final IntWritable VAL = new IntWritable();

		@Override
		protected void reduce(final PairOfStrings key, Iterable<Text> vals, final Context context) 
				throws IOException, InterruptedException {

			TObjectIntHashMap<String> idx = new TObjectIntHashMap<>();
			for (Text val : vals) {
				idx.adjustOrPutValue(val.toString(), 1, 1);
			}

			// Now if we are not crashed yet, move on
			TObjectIntHashMap<String> mi = combinations(idx);

			mi.forEachEntry(new TObjectIntProcedure<String>() {
				public boolean execute(String k, int v) {					
					try {
						KEY.set(k, key.getRightElement());
						VAL.set(v);
						context.write(KEY, VAL);						
					} catch (IOException | InterruptedException e) {					
						e.printStackTrace();
					}
					return true;
				}				
			});
		}
	}

	private static class MyPartitioner extends Partitioner<PairOfStrings, IntWritable> {

		@Override
		public int getPartition(PairOfStrings key, IntWritable val, int numReduceTasks) {
			return Math.abs(key.getLeftElement().hashCode() % Integer.MAX_VALUE) % numReduceTasks;
		}
	}

	private static class MyGroupingComparator extends WritableComparator {

		protected MyGroupingComparator() {
			super(PairOfStrings.class, true);
		}
		
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			PairOfStrings k1 = (PairOfStrings)a;
			PairOfStrings k2 = (PairOfStrings)b;
			return k1.getLeftElement().compareTo(k2.getLeftElement());
		}
	}

	private static class MySortingComparator extends WritableComparator {

		protected MySortingComparator() {
			super(PairOfStrings.class, true);
		}
		
		@Override
		public int compare(WritableComparable a, WritableComparable b) {	
			PairOfStrings k1 = (PairOfStrings)a;
			PairOfStrings k2 = (PairOfStrings)b;

			int compare = k1.getLeftElement().compareTo(k2.getLeftElement());			
			if (compare == 0) {
				return k1.getRightElement().compareTo(k2.getRightElement());
			}
			return compare;
		}
	}
	
	private static class MyReducer1 extends Reducer<PairOfStrings, IntWritable, Text, Text> {		

		private final Text KEY = new Text();
		private final Text VALUE = new Text();
		
		// This is nasty: We have to use global variables to do our job
		private DateTime begin = null, end = null, curDate = null;
		private String prevPair = null;
		private StringBuilder ts = new StringBuilder();
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);			
			Configuration conf = context.getConfiguration();
			begin = dtf.parseDateTime("2015-01-30");
			end = dtf.parseDateTime("2015-03-02");
		}

		@Override
		protected void reduce(PairOfStrings key, Iterable<IntWritable> vals, Context context)
						throws IOException, InterruptedException {
			
			// Sum before doing any fancy stuff
			int sum = 0;
			for (IntWritable v : vals) sum += v.get();
			
			if (prevPair == null || !prevPair.equals(key.getLeftElement())) {				
				curDate = begin;				
				if (prevPair != null) {
					KEY.set(prevPair);
					VALUE.set(ts.toString());
					ts.delete(0, ts.length());
					context.write(KEY, VALUE);
				}
				prevPair = key.getLeftElement();
				
				DateTime dt = dtf.parseDateTime(key.getRightElement());
				while (curDate.isBefore(dt)) {
					ts.append(String.valueOf(0));
					ts.append(' ');
					curDate = curDate.plusDays(1);
				}
				ts.append(String.valueOf(sum));
				ts.append(' ');
			}
			else {
				DateTime dt = dtf.parseDateTime(key.getRightElement());
				while (curDate.isBefore(dt)) {
					ts.append(String.valueOf(0));
					ts.append(' ');
					curDate = curDate.plusDays(1);
				}
				ts.append(String.valueOf(sum));
				ts.append(' ');
			}
		}
		
		@Override
		// Emit one last point
		protected void cleanup(Context context) throws IOException, InterruptedException {
			if (prevPair != null) {
				
				while (curDate.isBefore(end)) {
					ts.append(String.valueOf(0));
					ts.append(' ');
					curDate = curDate.plusDays(1);
				}
				
				KEY.set(prevPair);
				VALUE.set(ts.toString());
				context.write(KEY, VALUE);
			}
			ts.delete(0, ts.length());
			prevPair = null;
			begin = end = curDate = null;
		}
	}
	
	private static class MyReducer11 extends Reducer<PairOfStrings, IntWritable, Text, Text> {		

		private final Text KEY = new Text();
		private final Text VALUE = new Text();
		
		// This is nasty: We have to use global variables to do our job
		private DateTime begin = null, end = null, curDate = null;
		private String prevPair = null;
		private StringBuilder ts = new StringBuilder();
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);			
			Configuration conf = context.getConfiguration();
			begin = dtf.parseDateTime("2015-01-30");
			end = dtf.parseDateTime("2015-03-02");
		}

		@Override
		protected void reduce(PairOfStrings key, Iterable<IntWritable> vals, Context context)
						throws IOException, InterruptedException {
			
			// Sum before doing any fancy stuff
			int sum = 0;
			for (IntWritable v : vals) sum += v.get();			
			KEY.set(key.getLeftElement() + "\t" + key.getRightElement());
			VALUE.set(String.valueOf(sum));
			context.write(KEY, VALUE);
		}
		
		@Override
		// Emit one last point
		protected void cleanup(Context context) throws IOException, InterruptedException {			
			begin = end = curDate = null;
		}
	}

	@SuppressWarnings("static-access")
	@Override
	public Options options() {
		Options opts = super.options();
	
		Option pOpt = OptionBuilder.withArgName("phase").hasArg(true).isRequired(false)
				.withDescription("phase [aggr|cnt|ts]").create(PHASE);
		opts.addOption(pOpt);
		
		Option bOpt = OptionBuilder.withArgName("begin").hasArg(true).isRequired(false)
				.withDescription("beginning time (format YYYY-MM-dd)").create(BEGIN_TIME);
		opts.addOption(bOpt);
		
		Option eOpt = OptionBuilder.withArgName("end").hasArg(true).isRequired(false)
				.withArgName("ending time (format YYYY-MM-dd)").create(END_TIME);
		opts.addOption(eOpt);
		
		return opts;
	}
	@Override
	public int run(String[] args) throws Exception {
		parseOtions(args);
		String phase = "aggr";

		if (command.hasOption(PHASE)) {
			phase = command.getOptionValue(PHASE);
		}

		if ("aggr".equals(phase)) {
			phase1(args);
		}
		else if ("cnt".equals(phase)) {
			phase2(args);
		}
		else if ("ts".equals(phase)) {
			phase3(args, false);
		}
		else if ("tst".equals(phase)) {
			phase3(args, true);
		}
		return 0;
	}

	private int phase1(String[] args) throws InterruptedException,
	IOException, ClassNotFoundException {
		System.out.println("Phase 1");
		Job job = setup(TextInputFormat.class, SequenceFileOutputFormat.class,
				PairOfStrings.class, Text.class, PairOfStrings.class, IntWritable.class,
				MyMapper.class, MyReducer.class, args);
		job.waitForCompletion(true);
		return 0;
	}

	private int phase2(String[] args) throws InterruptedException,
	IOException, ClassNotFoundException {
		System.out.println("Phase 2");
		Job job = setup(SequenceFileInputFormat.class, SequenceFileOutputFormat.class,
				PairOfStrings.class, IntWritable.class, PairOfStrings.class, IntWritable.class,
				Mapper.class, IntSumReducer.class, IntSumReducer.class, args);
		job.waitForCompletion(true);
		return 0;
	}

	private int phase3(String[] args, boolean test) throws InterruptedException,
	IOException, ClassNotFoundException {
		System.out.println("Phase 3");
		Job job = setup(SequenceFileInputFormat.class, TextOutputFormat.class,
				PairOfStrings.class, IntWritable.class, PairOfStrings.class, IntWritable.class,
				Mapper.class, (test) ? MyReducer11.class : MyReducer1.class, args);
		Configuration conf = job.getConfiguration();
		
		String beginTime = "2015-01-30";
		if (command.hasOption(BEGIN_TIME)) {
			beginTime = command.getOptionValue(BEGIN_TIME);
			conf.set(BEGIN_TIME, beginTime);
		}
		
		String endTime = "2015-03-02";
		if (command.hasOption(END_TIME)) {
			endTime = command.getOptionValue(END_TIME);
			conf.set(END_TIME, endTime);
		}
		
		job.setPartitionerClass(MyPartitioner.class);
		job.setGroupingComparatorClass(MyGroupingComparator.class);
		job.setSortComparatorClass(MySortingComparator.class);
		
		job.waitForCompletion(true);
		return 0;
	}

	private static TObjectIntHashMap<String> combinations(TObjectIntHashMap<String> maps) {
		String[] keys = maps.keys(new String[maps.size()]);
		Arrays.sort(keys);		
		TObjectIntHashMap<String> results = new TObjectIntHashMap<>(maps.size() * (maps.size()-1)/2);

		for (int i=0; i<keys.length-1;i++) {			
			for (int j=i+1; j<keys.length;j++) {
				if (keys[i].equals(keys[j])) continue;
				String k = keys[i] + "@@" + keys[j];
				results.adjustOrPutValue(k, Math.min(maps.get(keys[i]), maps.get(keys[j])), 
						Math.min(maps.get(keys[i]), maps.get(keys[j])));
			}
		}
		return (results.size() == 0) ? null : results;
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new BuildShortTermTS(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}