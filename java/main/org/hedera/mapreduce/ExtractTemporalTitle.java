/**
 * 
 */
package org.hedera.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import tuan.hadoop.conf.JobConfig;

/**
 * Extract titles for every revisions and timestamps of Wikipedia revision
 * @author tuan
 *
 */
public class ExtractTemporalTitle extends JobConfig implements Tool {

	public static final class MyMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	
		private final Text KEY = new Text();
		private JsonParser parser;

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {			
			JsonObject obj =
					(JsonObject) parser.parse(value.toString());

			long pageId = obj.get("page_id").getAsLong();
			long revisionId = obj.get("rev_id").getAsLong();
			long timestamp = obj.get("timestamp").getAsLong();
			String title = obj.get("page_title").getAsString();
			
			KEY.set(pageId + "\t" + revisionId + "\t" + timestamp + "\t" + title);
			context.write(KEY, NullWritable.get());
		}
		
		@Override
		protected void setup(Context context) throws IOException,
		InterruptedException {
			parser = new JsonParser();
		}

		@Override
		protected void cleanup(Context context) throws IOException,
		InterruptedException {
			parser = null;
		} 
	}
	
	@Override
	public int run(String[] args) throws Exception {		
		Job job = setup(TextInputFormat.class, TextOutputFormat.class, Text.class,
				NullWritable.class, Text.class, NullWritable.class,
				MyMapper.class, Reducer.class, args);

		job.waitForCompletion(false);
		return 0;
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new ExtractTemporalTitle(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
