/**
 * 
 */
package org.hedera.mapreduce.experiments;



import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hedera.io.WikipediaRevision;
import org.hedera.io.input.WikiRevisionPageInputFormat;

import pignlproc.markup.AnnotatingMarkupParser;
import tuan.hadoop.conf.JobConfig;

/**
 * Generate document length data for Okapi-BM25 scores
 * @author tuan
 *
 */
public class WikiRevLength extends JobConfig implements Tool  {

	private static final class IndexMapper extends Mapper<LongWritable, 
			WikipediaRevision, Text, Text> {
		
		private Text keyOut = new Text();
		private Text valOut = new Text();
		
		private AnnotatingMarkupParser parser;
		private static final Pattern SPACES_PATTERN = Pattern.compile("\\s+");
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			parser = new AnnotatingMarkupParser("en");
		}

		@Override
		protected void map(LongWritable key, WikipediaRevision value,
				final Context context) throws IOException, InterruptedException {
			
			final long timestamp = value.getTimestamp();
			
			// internal map			
			// each line here is a performance / memory killer
			String rawText = new String(value.getText(), "UTF-8");			
			String text = parser.parse(rawText);
			
			String[] tokens = SPACES_PATTERN.split(text);
			
			keyOut.set(String.valueOf(key.get()));
			valOut.set(timestamp + "\t" + tokens.length);
			context.write(keyOut, valOut);
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		String inputDir = args[0];
		String outputDir = args[1];
		
		Job job = setup("Indexing document length for OkapiBM25", 
				WikiRevLength.class, inputDir, outputDir,
				WikiRevisionPageInputFormat.class, TextOutputFormat.class, 
				Text.class, Text.class, Text.class, Text.class,
				IndexMapper.class, Reducer.class, 1);
		job.waitForCompletion(true);
		return 0;
	}
	
	public static void main(String[] args) {
		try {
			ToolRunner.run(new WikiRevLength(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
