/**
 * 
 */
package org.hedera.mapreduce.experiments;

import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.procedure.TObjectIntProcedure;

import java.io.IOException;
import java.util.Locale;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.hedera.io.WikiRevisionWritable;
import org.hedera.io.input.WikiRevisionPageInputFormat;

import pignlproc.markup.AnnotatingMarkupParser;
import tuan.hadoop.conf.JobConfig;
import tuan.hadoop.io.LongTripleWritable;

/**
 * Build a simple inverted index for word-based search without
 * temporal features. Posting format:
 * [word, pageId, timestamp, cnt]
 * @author tuan
 */
public class WikiRevIndex4NonTemporalSearch extends JobConfig implements Tool {

	private static final class IndexMapper extends Mapper<LongWritable, 
			WikiRevisionWritable, Text, LongTripleWritable> {

		private Logger LOG = Logger.getLogger(WikiRevIndex4NonTemporalSearch.class);
		
		private Text keyOut = new Text();
		private LongTripleWritable valOut = new LongTripleWritable();
		
		private AnnotatingMarkupParser parser;
		private static final Pattern SPACES_PATTERN = Pattern.compile("\\s+");
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			parser = new AnnotatingMarkupParser("en");
		}

		@Override
		protected void map(LongWritable key, WikiRevisionWritable value,
				final Context context) throws IOException, InterruptedException {
			
			final long pageId = key.get();
			final long timestamp = value.getTimestamp();
			
			// internal map
			TObjectIntHashMap<String> internalCounter = new TObjectIntHashMap<>();
			
			// each line here is a performance / memory killer
			String rawText = new String(value.getText(), "UTF-8");			
			String text = parser.parse(rawText);
			rawText = null;
			
			String[] tokens = SPACES_PATTERN.split(text);
			for (String token : tokens) {
				String word = token.toLowerCase(Locale.ENGLISH);
				LOG.info("processing " + word + " at revision . " + value.getRevisionId());
				internalCounter.adjustOrPutValue(word, 1, 1);
			}
			
			internalCounter.forEachEntry(new TObjectIntProcedure<String>() {
				public boolean execute(String w, int cnt) {
					keyOut.set(w);
					valOut.set(pageId, timestamp, cnt);
					try {
						context.write(keyOut, valOut);
					} catch (IOException | InterruptedException e) {
						LOG.error("cannot emit the term " + w, e);
						return true;
					}
					return true;
				}
			});
		}
	}
	
	private static final class IndexReducer extends 
			Reducer<Text, LongTripleWritable, Text, Text> {

		private Text keyOut = new Text();
		private Text valOut = new Text();
		
		@Override
		protected void reduce(Text key, Iterable<LongTripleWritable> vals,
				Context c) throws IOException, InterruptedException {
			keyOut.set(key);
			for (LongTripleWritable val : vals) {
				valOut.set(val.firstElement() + "\t" + val.secondElement() + "\t"
						+ val.thirdElement());
				c.write(keyOut, valOut);
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		String inputDir = args[0];
		String outputDir = args[1];
		int reduceNo = Integer.parseInt(args[2]);
		
		Job job = setup("Baseline 1: Indexing revisions for OkapiBM25", 
				WikiRevIndex4NonTemporalSearch.class, inputDir, outputDir,
				WikiRevisionPageInputFormat.class, TextOutputFormat.class, 
				Text.class, LongTripleWritable.class, Text.class, Text.class,
				IndexMapper.class, IndexReducer.class, reduceNo);
		job.waitForCompletion(true);
		return 0;
	}
	
	public static void main(String[] args) {
		try {
			ToolRunner.run(new WikiRevIndex4NonTemporalSearch(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
