package org.hedera.mapreduce;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import tl.lin.data.array.ArrayListWritable;
import tuan.hadoop.conf.JobConfig;

/**
 * Extract internal and external references from Wikipedia project
 */
public class ExtractReferences extends JobConfig implements Tool {

	private static final class MyMapper extends 
			Mapper<LongWritable, Text, Long, ArrayListWritable<Text>> {

		private JsonParser parser;
		private static final LongWritable keyOut = new LongWritable();
		private static final ArrayListWritable<Text> valOut = new ArrayListWritable<Text>();
		
		private static final Pattern HTTP_PATTERN = Pattern.compile("http://\\S+?\\s");
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			JsonObject obj =
					(JsonObject) parser.parse(value.toString());
			long pageId = obj.get("page_id").getAsLong();
			keyOut.set(pageId);
			String content = obj.get("text").getAsString();
			
			extractReferences(content);
			
			context.write(pageId, valOut);
		}
		
		private void extractReferences(String content) {
			
			// clean the output
			valOut.clear();
			
			Matcher m = HTTP_PATTERN.matcher(content);
			while (m.find()) {
				valOut.add(new Text(m.group(0)));
			}
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
		Job job = setup(TextInputFormat.class, TextOutputFormat.class, LongWritable.class,
				ArrayListWritable.class, LongWritable.class, ArrayListWritable.class,
				MyMapper.class, Reducer.class, args);
		
		job.waitForCompletion(true);
		
		return 0;
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new ExtractReferences(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
