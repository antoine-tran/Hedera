package org.hedera.mapreduce;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hedera.io.WikipediaRevisionDiff;
import org.hedera.io.input.WikiRevisionDiffInputFormat;
import org.hedera.io.input.WikiRevisionInputFormat;
import org.hedera.io.input.WikiRevisionReader;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import com.google.common.collect.Lists;

import difflib.Chunk;
import difflib.Delta;
import edu.umd.cloud9.io.pair.PairOfLongs;
import tuan.hadoop.conf.JobConfig;
import static org.hedera.io.input.WikiRevisionInputFormat.TIME_FORMAT;
import static org.hedera.io.WikipediaRevisionDiff.opt2byte; 

/**
 * This jobs extract temporal anchor text from Wikipedia revisions 
 * @author tuan
 *
 */
public class ExtractTemporalAnchorText extends JobConfig implements Tool {

	private static final DateTimeFormatter dtf = ISODateTimeFormat.dateTimeNoMillis();

	// Algorithm:
	// emit (title, 0) --> (doc id, timestamp, timfor structure message (
	private static final class MyMapper extends Mapper<LongWritable, WikipediaRevisionDiff, 
	PairOfLongs, Text> {

		private PairOfLongs keyOut = new PairOfLongs();
		private Text valOut = new Text();

		@Override
		protected void setup(Context context) throws IOException,
		InterruptedException {
			super.setup(context);
		}

		@Override
		protected void map(LongWritable key, WikipediaRevisionDiff value,
				Context context) throws IOException, InterruptedException {

			// skip non-article pages
			int namespace = value.getNamespace();			
			if (namespace != 0) {
				return;				
			}

			long pageId = value.getPageId();
			long timestamp = value.getTimestamp();
			long revId = value.getRevisionId();
			long parId = value.getParentId();
			String title = value.getPageTitle();
			LinkedList<Delta> diffs = value.getDiffs();

			keyOut.set(revId, timestamp);

			if (diffs != null) {
				for (Delta diff : diffs) {
					Chunk revi = diff.getRevised();
					List<?> texts = revi.getLines();
					for (Object obj : texts) {
						String text = (String)obj;
						List<Link> links = extractLinks(text);
						for (Link link : links) {
							StringBuilder sb = new StringBuilder();
							String ts = TIME_FORMAT.print(timestamp);
							sb.append(ts);
							sb.append("\t");
							sb.append(pageId);
							sb.append("\t");
							sb.append(revId);
							sb.append("\t");
							sb.append(parId);
							sb.append("\t");
							sb.append(opt2byte(diff.getType()));
							sb.append("\t");
							sb.append(title);
							sb.append("\t");
							sb.append(link.anchor);
							sb.append("\t");
							sb.append(link.target);
							sb.append("\t");
							valOut.set(sb.toString());
							context.write(keyOut, valOut);
						}
					}
				}
			}	
		}

		private List<Link> extractLinks(String page) {
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
				links.add(new Link(anchor, text));

				start = end + 1;
			}

			return links;
		}
	}	

	public static class Link {
		private String anchor;
		private String target;

		private Link(String anchor, String target) {
			this.anchor = anchor;
			this.target = target;
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
	}

	@Override
	public int run(String[] args) throws Exception {
		String inputDir = args[0];
		String outputDir = args[1];
		int reduceNo = Integer.parseInt(args[2]);
		
		// this job sucks big memory
		setMapperSize("-Xmx5120m");
		
		Job job = setup("For Avishek: Extracting temporal anchor text from "
				+ "Wikipedia revision",
				ExtractTemporalAnchorText.class, inputDir, outputDir,
				WikiRevisionDiffInputFormat.class, TextOutputFormat.class,
				PairOfLongs.class, Text.class, PairOfLongs.class, Text.class,
				MyMapper.class, Reducer.class, reduceNo);
		
		// skip non-article
		getConf().setBoolean(WikiRevisionInputFormat.SKIP_NON_ARTICLES, true);
		
		job.waitForCompletion(true);
		return 0;
	}
	
	public static void main(String[] args) {
		try {
			ToolRunner.run(new ExtractTemporalAnchorText(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
