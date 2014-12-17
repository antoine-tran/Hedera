/**
 * 
 */
package org.hedera.mapreduce;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
import edu.umd.cloud9.collection.wikipedia.WikipediaPage.Link;
import edu.umd.cloud9.io.pair.PairOfStringInt;
import edu.umd.cloud9.io.pair.PairOfStrings;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.procedure.TIntProcedure;
import tuan.hadoop.conf.JobConfig;

/**
 * @author tuan
 *
 */
public class EntityCoocurrenceInDisambiguation extends JobConfig implements Tool {

	private static final String MAPPER_SIZE = "-Xmx2048m"; 
	private static final String PHASE = "phase";
	private static final String REDIR_OPT = "redir";
	
	private static enum PageTypes {
		TOTAL, REDIRECT, DISAMBIGUATION, EMPTY, ARTICLE, STUB, NON_ARTICLE
	};

	// Input: Wikipedia page, output: a word in the title of the disambiguation, list of pages
	private static final class DisambMapper extends Mapper<IntWritable, WikipediaPage, 
	PairOfStringInt, Text> {

		private static final PairOfStringInt KEYPAIR = new PairOfStringInt();
		private static final Text VALUEPAIR = new Text();

		@Override

		// Basic algorithm
		// Emit message 1 : key = (link target article name, 0), value = (link target docid);
		// Emit message 2 : key = (link target article name, 1), value = (label); 
		protected void map(IntWritable key, WikipediaPage p, Context context) 
				throws IOException, InterruptedException {

			context.getCounter(PageTypes.TOTAL).increment(1);

			String title = p.getTitle();

			// This is a caveat and a potential gotcha: Wikipedia article titles are not case sensitive on
			// the initial character, so a link to "commodity" will go to the article titled "Commodity"
			// without any issue. Therefore we need to emit two versions of article titles.

			VALUEPAIR.set(p.getDocid());
			KEYPAIR.set(title, 0);
			context.write(KEYPAIR, VALUEPAIR);

			String fc = title.substring(0, 1);
			if (fc.matches("[A-Z]")) {
				title = title.replaceFirst(fc, fc.toLowerCase());

				KEYPAIR.set(title, 0);
				context.write(KEYPAIR, VALUEPAIR);
			}

			if (p.isRedirect()) {
				context.getCounter(PageTypes.REDIRECT).increment(1);
			} else if (p.isDisambiguation()) {
				context.getCounter(PageTypes.DISAMBIGUATION).increment(1);
			} else if (p.isEmpty()) {
				context.getCounter(PageTypes.EMPTY).increment(1);
			} else if (p.isArticle()) {
				context.getCounter(PageTypes.ARTICLE).increment(1);

				if (p.isStub()) {
					context.getCounter(PageTypes.STUB).increment(1);
				}
			} else {
				context.getCounter(PageTypes.NON_ARTICLE).increment(1);
			}

			if (title.endsWith("(disambiguation)")) {
				int i = title.indexOf(" (disambiguation)");
				if (i < 0) {
					return;
				}
				String label = title.substring(0, i);
				for (Link link : p.extractLinks()) {
					KEYPAIR.set(link.getTarget(), 1);
					VALUEPAIR.set(label);
					context.write(KEYPAIR, VALUEPAIR);
				}
			}
		}
	}

	// In reduce phase, we ignore all links not refered to by any disambiguation pag
	// Note: We downcast doc id to Integer to match against the redirect map in the later phase
	private static final class DisambReducer extends Reducer<PairOfStringInt, Text, IntWritable, Text> {

		private static final IntWritable OUTKEY = new IntWritable();

		private String targetTitle;
		private int targetDocid;

		@Override
		protected void reduce(PairOfStringInt key, Iterable<Text> vals, Context context)
				throws IOException, InterruptedException {

			if (key.getRightElement() == 0) {
				targetTitle = key.getLeftElement();
				for (Text val : vals) {
					targetDocid = Integer.parseInt(val.toString());
					break;
				}
			}
			else {

				// Which case is this ?
				if (!key.getLeftElement().equals(targetTitle)) {
					return;
				}

				OUTKEY.set(targetDocid);
				for (Text val : vals) {
					context.write(OUTKEY, val);
				}
			}
		}		
	}

	private static class MyPartitioner extends Partitioner<PairOfStringInt, PairOfStrings> {
		public int getPartition(PairOfStringInt key, PairOfStrings value, int numReduceTasks) {
			return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
		}
	}

	private void resolveRedirect(String inPath, String redirectPath, String outPath) throws IOException {

		// caches
		IntWritable targetId = new IntWritable();
		Text disamb = new Text();
		IntWritable target = new IntWritable(0);

		// TODO: this map in a way forms a local reduce phase. Be careful with too big map
		Map<String, TIntArrayList> disambMap = new HashMap<String, TIntArrayList>();

		// read the redirect file
		MapFile.Reader redirectReader = null;
		SequenceFile.Reader disambReader = null;
		BufferedWriter writer = null;

		FileSystem fs = FileSystem.get(getConf());

		try {
			disambReader = new SequenceFile.Reader(getConf(), 
					Reader.file(new Path(inPath + "/part-r-00000")));
			redirectReader = new MapFile.Reader(new Path(redirectPath), getConf());

			while(disambReader.next(targetId, disamb)) {
				String l = disamb.toString();
				if (!disambMap.containsKey(l)) {
					disambMap.put(l, new TIntArrayList());
				}
				TIntArrayList v = disambMap.get(l);
				redirectReader.get(targetId, target);
				if (target.get() > 0) {
					v.add(target.get());
				}
				else {
					v.add(targetId.get());
				}
			}

		} finally {
			if (disambReader != null) disambReader.close();
			if (redirectReader != null) redirectReader.close();
		}

		try {
			FSDataOutputStream os = fs.create(new Path(outPath), true);			
			writer = new BufferedWriter(new OutputStreamWriter(os));
			final StringBuilder sb = new StringBuilder();
			for (String k : disambMap.keySet()) {
				sb.delete(0, sb.length());
				sb.append(k);
				sb.append("\t");
				TIntArrayList lst = disambMap.get(k);
				lst.forEach(new TIntProcedure() {
					public boolean execute(int item) {
						sb.append(item);
						sb.append(',');
						return true;
					}
				});
				sb.append('\n');
				writer.write(sb.toString());
			}
		}
		finally {
			if (writer != null) writer.close();

			// Clean up intermediate data.
			FileSystem.get(getConf()).delete(new Path(inPath), true);
		}
	}

	@SuppressWarnings("static-access")
	@Override
	public Options options() {
		Options opts = super.options();
		opts.addOption(OptionBuilder.withArgName("phase").hasArg()
				.withDescription("phase (1/2)").create(PHASE));
		opts.addOption(OptionBuilder.withArgName("redir").hasArg()
				.withDescription("path to redirect file").create(REDIR_OPT));
		return opts;
	}


	// Count entities by days
	public int phase1(String tmp) throws IOException {
		Job job = setup(jobName + ": Phase 1", EntityCoocurrenceInDisambiguation.class,
				input, tmp,
				SequenceFileInputFormat.class, SequenceFileOutputFormat.class,
				PairOfStringInt.class, Text.class,
				IntWritable.class, Text.class,
				DisambMapper.class, 
				DisambReducer.class, 1);
		job.setPartitionerClass(MyPartitioner.class);
		configureJob(job);
		try {
			job.waitForCompletion(true);
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}
		return 0;
	}
	
	public int phase2(String tmp, String redirPath) {
		try {
			resolveRedirect(tmp, redirPath, output);
			return 0;
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}
	}

	private void configureJob(Job job) {
		job.getConfiguration().set("mapreduce.map.memory.mb", "4096");
		job.getConfiguration().set("mapreduce.reduce.memory.mb", "4096");
		job.getConfiguration().set("mapreduce.map.java.opts", "-Xmx4096m");
		job.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx4096m");
		job.getConfiguration().set("mapreduce.job.user.classpath.first", "true");		
	}

	@Override
	public int run(String[] args) throws Exception {
		parseOtions(args);
		setMapperSize(MAPPER_SIZE);
				
		int res = 0;
		
		int phase = Integer.parseInt(command.getOptionValue(PHASE));
		if (phase == 1) {
			res = phase1(output);
		}
		else {
			String redirPath = command.getOptionValue(REDIR_OPT);
			Random random = new Random();
			String tmp = "tmp-" + this.getClass().getCanonicalName() + "-" + random.nextInt(10000);
			
			res &= phase1(tmp);
			res &= phase2(tmp, redirPath);
		}
		
		return res;
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new EntityCoocurrenceInDisambiguation(), args);
		} catch (Exception e) {		
			e.printStackTrace();
		}
	}
}
