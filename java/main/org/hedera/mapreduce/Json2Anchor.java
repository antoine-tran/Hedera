package org.hedera.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import tuan.hadoop.conf.JobConfig;

public class Json2Anchor extends JobConfig implements Tool {

	private static final class MyMapper extends 
	Mapper<LongWritable, Text, LongWritable, Text> {

		private JsonParser parser;
		private static final LongWritable keyOut = new LongWritable();
		private static final Text valOut = new Text();
		private static final StringBuilder sb = new StringBuilder();

		private static final Pattern HTTP_PATTERN = Pattern.compile("http://\\S+?\\s");

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			JsonObject obj =
					(JsonObject) parser.parse(value.toString());

			long pageId = obj.get("page_id").getAsLong();
			long revisionId = obj.get("rev_id").getAsLong();
			long timestamp = obj.get("timestamp").getAsLong();

			keyOut.set(timestamp);
			String content = obj.get("text").getAsString();

			extractAnchor(pageId,revisionId,content, context);
		}

		// Scala: Mutability sucks. Me: Scala sucks !!
		private void extractAnchor(long pageId, long revisionId,
								   String content, Context context)
				throws IOException, InterruptedException {

			// clean the output
			valOut.clear();
			sb.delete(0, sb.length());
			sb.append(pageId);
			sb.append('\t');
			sb.append(revisionId);
			sb.append('\t');

			int offset = sb.length();

			List<Link> lst = extractLinks(content);
			for (Link l : lst) {
				sb.append(l.getTarget());
				sb.append('\t');
				sb.append(l.getAnchorText());
				sb.append('\t');
				sb.append(l.getOffset());
				sb.append('\t');
				sb.append(l.getPreContext());
				sb.append('\t');
				sb.append(l.getPostContext());
				valOut.set(sb.toString());
				sb.delete(offset, sb.length());
				context.write(keyOut,valOut);
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
	
	public static List<Link> extractLinks(String page) {
		int start = 0;
		List<Link> links = Lists.newArrayList();

		while (true) {
			start = page.indexOf("[[", start);

			if (start < 0) {
				break;
			}

			int end = page.indexOf("]]", start);

			if (end < 0) {
				break;
			}

			String text = page.substring(start + 2, end);
			String preContext = page.substring(Math.max(start-20, 0), start).replace('\n', ' ').replace('\t',' ');
			String postContext = page.substring(Math.min(end+2,page.length()),
					Math.min(end+22,page.length())).replace('\n', ' ').replace('\t',' ');
			String anchor = null;

			// skip empty links
			if (text.length() == 0) {
				start = end + 1;
				continue;
			}

			// skip special links
			if (text.indexOf(":") != -1) {
				start = end + 1;
				continue;
			}

			// if there is anchor text, get only article title
			int a;
			if ((a = text.indexOf("|")) != -1) {
				anchor = text.substring(a + 1, text.length());
				text = text.substring(0, a);
			}

			if ((a = text.indexOf("#")) != -1) {
				text = text.substring(0, a);
			}

			// ignore article-internal links, e.g., [[#section|here]]
			if (text.length() == 0) {
				start = end + 1;
				continue;
			}

			if (anchor == null) {
				anchor = text;
			}
			Link link = new Link(anchor, text, start+2);
			link.setPreContext(preContext);
			link.setPostContext(postContext);
			links.add(link);

			start = end + 1;
		}

		return links;
	}

	public static class Link {
		private int offset;
		private String anchor;
		private String target;
		private String preContext;
		private String postContext;

		private Link(String anchor, String target) {
			this.anchor = anchor;
			this.target = target;
		}

		private Link(String anchor, String target, int offset) {
			this.anchor = anchor;
			this.target = target;
			this.offset = offset;
		}

		public void setOffset(int offset) {
			this.offset = offset;
		}

		public int getOffset() {
			return offset;
		}

		public String getAnchorText() {
			return anchor;
		}

		public String getTarget() {
			return target;
		}

		public String toString() {
			return String.format("[target: %s, anchor: %s]", target, anchor);
		}

		public void setPreContext(String p) {
			this.preContext = p;
		}

		public void setPostContext(String c) {
			this.postContext = c;
		}

		public String getPreContext() {
			return preContext;
		}

		public String getPostContext() {
			return postContext;
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Job job = setup(TextInputFormat.class, TextOutputFormat.class, LongWritable.class,
				Text.class, LongWritable.class, Text.class,
				MyMapper.class, Reducer.class, args);

		job.waitForCompletion(true);

		return 0;
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new Json2Anchor(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

