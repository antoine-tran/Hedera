package org.hedera.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hedera.io.RevisionBOW;
import org.hedera.io.etl.RevisionBOWInputFormat;
import org.hedera.io.input.WikiRevisionInputFormat;

import edu.umd.cloud9.io.array.ArrayListOfLongsWritable;
import edu.umd.cloud9.io.pair.PairOfLongs;
import edu.umd.cloud9.util.map.MapKI.Entry;

import tuan.hadoop.conf.JobConfig;

/** Build a simple inverted index from revisions' bag-of-words.
 * Output format:
 * [Word] TAB [pageId TAB beginTimestamp TAB endTimestamp TAB count] */
public class InvertedIndexByBOW extends JobConfig implements Tool {

	private static final class MyMapper extends Mapper<LongWritable,
			RevisionBOW, Text, ArrayListOfLongsWritable> {

		// emit for each word
		private Text keyOut = new Text();
		
		// pageid,  beginTimestamp, endTimestamp, count
		private ArrayListOfLongsWritable valOut = new ArrayListOfLongsWritable(4);
		
		@Override
		protected void map(LongWritable key, RevisionBOW value, Context context)
				throws IOException, InterruptedException {
			
			Iterator<Entry<String>> words = value.getWords();
			while (words.hasNext()) {
				Entry<String> w = words.next();
				keyOut.set(w.getKey());
				valOut.set(0, key.get());
				valOut.set(1, value.getLastTimestamp());
				valOut.set(2, value.getTimestamp());
				valOut.set(3, w.getValue());
				context.write(keyOut, valOut);
			}	
		}
	}
	
	private static final class MyReducer extends 
			Reducer<Text, ArrayListOfLongsWritable, Text, Text> {

		private Text valOut = new Text();
		
		@Override
		protected void reduce(Text key,Iterable<ArrayListOfLongsWritable> vals,
				Context c)
				throws IOException, InterruptedException {
			for (ArrayListOfLongsWritable v : vals) {
				valOut.set(v.get(0) + "\t" + v.get(1) + "\t"
						+ v.get(2) + "\t" + v.get(3));
				c.write(key,valOut);
			}
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		String inputDir = args[1];
		String outputDir = args[2];
		String name = args[0];
		int reduceNo = Integer.parseInt(args[3]);

		// this job sucks big memory
		setMapperSize("-Xmx5120m");

		// skip non-article and redirect pages
		getConf().setBoolean(WikiRevisionInputFormat.SKIP_NON_ARTICLES, true);
		getConf().setBoolean(WikiRevisionInputFormat.SKIP_REDIRECT, true);

		Job job = setup("Hedera: " + name,
				InvertedIndexByBOW.class, inputDir, outputDir,
				RevisionBOWInputFormat.class, TextOutputFormat.class,
				Text.class, ArrayListOfLongsWritable.class,
				Text.class, Text.class,
				MyMapper.class, MyReducer.class, reduceNo);

		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new InvertedIndexByBOW(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
