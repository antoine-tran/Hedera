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
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import tuan.collections.CharArrayList;
import tuan.hadoop.conf.JobConfig;

/**
 * Convert anchor data into freebase id. Prepended text:
 * timestamp SourceEntNum DestEntNum .....
 * @author tuan
 *
 */
public class Anchor2Freebase extends JobConfig implements Tool {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			ToolRunner.run(new Anchor2Freebase(), args);
		} catch (Exception e) {			
			e.printStackTrace();
		}
	}

	/** Tells whether a char is in a string*/
	private static boolean in(char c, String s) {
		return (s.indexOf(c) != -1);
	}

	/** Tells whether a char is in a range*/
	private static boolean in(char c, char a, char b) {
		return (c >= a && c <= b);
	}

	/** Tells whether a char is alphanumeric in the sense of URIs */
	private static boolean isAlphanumeric(char c) {
		return (in(c, 'a', 'z')
				|| in(c, 'A', 'Z') || in(c, '0', '9'));
	}

	private static void encode(char c, CharArrayList lst) {
		if (!isAlphanumeric(c) && !in(c,"_-")) {
			String hex = Integer.toHexString(c).toUpperCase();
			int n = hex.length();
			lst.add('$');
			while (n < 4) {
				lst.add('0');
				n++;
			}
			n = 0;
			while (n < hex.length()) {
				lst.add(hex.charAt(n));
				n++;
			}
		}
		else {
			lst.add(c);
		}
	}

	private static String encodeFreebase(String inp) {
		CharArrayList buffer = new CharArrayList();
		for (int i = 0; i < inp.length(); i++) {
			encode(inp.charAt(i), buffer);
		}
		return new String(buffer.toArray());
	}

	// Encode anchors into freebase-ready format
	// Version for incremented anchor data set
	private static final class GroupTitleMapperNew extends Mapper<LongWritable, Text,
			Text, Text> {

		private final Text KEY = new Text();

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			int i = line.indexOf('\t');
			int j = line.indexOf('\t', i + 1);
			int k = line.indexOf('\t', j + 1);
			int l = line.indexOf('\t', k + 1);

			// String encoded = encodeFreebase(line.substring(k + 1, l).trim().replace(' ', '_'));
			try {
				String t = line.substring(k + 1, l).trim();
				if (t.length() > 0) {
					KEY.set(t.replace(' ', '_'));
					context.write(KEY, value);	
				}
			}
			catch (Exception e) {
				// System.out.println(line);
			}
		}
	}

	private static final class EncodeReducer extends Reducer<Text, Text,
			Text, Text> {

		private static final Text KEY = new Text();

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String anchor = key.toString();
			
			if (anchor != null && anchor.length() >= 2) {
				String encoded = encodeFreebase(anchor);	
				KEY.set(encoded);			
			}
			else KEY.set("WTF");
			for (Text v : values)
				context.write(KEY,v);
		}
	}

	@Override
	// Step 1: Encode titles into Freebase format, has been written in Java
	// Step 2: Join encoded titles with Freebase label dictionaries to convert
	// the graphs into freebaseID
	public int run(String[] args) throws Exception {
		Job job = setup(TextInputFormat.class, TextOutputFormat.class,
				Text.class, Text.class, Text.class, Text.class,
				GroupTitleMapperNew.class, EncodeReducer.class, args);
		job.waitForCompletion(true);
		return 0;
	}
}
