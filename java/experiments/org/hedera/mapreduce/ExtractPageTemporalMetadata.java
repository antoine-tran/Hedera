/**
 * 
 */
package org.hedera.mapreduce;

import java.io.IOException;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.hedera.io.RevisionHeader;
import org.hedera.io.input.WikiRevisionHeaderJsonInputFormat;

import tuan.hadoop.conf.JobConfig;

/**
 * Extract different temporal metadata (creation date, etc. ) of a Wikipedia
 * page
 * 
 * @author tuan
 *
 */
public class ExtractPageTemporalMetadata extends JobConfig implements Tool {

	/** Extra options: the metadata wanted */
	private static final String EXTRACT_JOB_OPT = "job";
	private static final String MAPPER_SIZE = "-Xmx4096m"; 
		
	private static final class CreationMapper 
			extends Mapper<LongWritable, RevisionHeader, LongWritable, LongWritable> {

		private final LongWritable keyOut = new LongWritable();
		private final LongWritable valOut = new LongWritable();
		
		@Override
		protected void map(LongWritable key, RevisionHeader header,
				Context context) throws IOException, InterruptedException {
			long revisionId = header.getRevisionId();
			valOut.set(revisionId);
			long pageId = header.getPageId();
			keyOut.set(pageId);
			
			context.write(keyOut, valOut);
		}	
	}
	
	private static final class LongMinReducer<T> 
			extends Reducer<T, LongWritable, T, LongWritable> {

		private final LongWritable value = new LongWritable(0L);
		
		@Override
		protected void reduce(T k, Iterable<LongWritable> vals, Context c)
				throws IOException, InterruptedException {
			value.set(0L);
			for (LongWritable item : vals) {
				if (value.compareTo(item) > 0) {
					value.set(item.get());
				}
			}
			c.write(k, value);
		}
	}
	
	@SuppressWarnings("static-access")
	@Override
	public Options options() {
		Options opts = super.options();
		Option jobOpt = OptionBuilder.withArgName("extraction-job").hasArg(true)
				.withDescription("The extraction job [creation | sample ]")
				.create(EXTRACT_JOB_OPT);
		opts.addOption(jobOpt);
		return opts;
	}
	
	private void configureJob(Job job) {
		job.getConfiguration().set("mapreduce.map.memory.mb", "4096");
		job.getConfiguration().set("mapreduce.reduce.memory.mb", "4096");
		job.getConfiguration().set("mapreduce.map.java.opts", "-Xmx4096m");
		job.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx4096m");
		job.getConfiguration().set("mapreduce.job.user.classpath.first", "true");		
	}
	
	private int extractPageCreation() throws IOException {
		Job job = setup("Hedera: Extracting page creation time",
				ExtractPageTemporalMetadata.class,
				input, output, 
				WikiRevisionHeaderJsonInputFormat.class,
				TextOutputFormat.class,
				LongWritable.class, RevisionHeader.class, 
				LongWritable.class, LongWritable.class,
				CreationMapper.class, LongMinReducer.class, LongMinReducer.class,
				1);
		configureJob(job);
		try {
			job.waitForCompletion(true);
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}
		return 0;
	}

	@Override
	public int run(String[] args) throws Exception {
		parseOtions(args);
		setMapperSize(MAPPER_SIZE);
		
		if (!command.hasOption(EXTRACT_JOB_OPT)) {
			System.err.println("Must specify the extraction job option");
		}
		
		String jobOpt = command.getOptionValue(EXTRACT_JOB_OPT);
		
		if ("creation".equals(jobOpt)) {
			return extractPageCreation();
		}
		
		return 0;
	}
}
