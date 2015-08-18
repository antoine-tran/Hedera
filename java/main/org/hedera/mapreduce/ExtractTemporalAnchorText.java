package org.hedera.mapreduce;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hedera.io.RevisionDiff;
import org.hedera.io.input.WikiRevisionDiffInputFormat;
import org.hedera.io.input.WikiRevisionInputFormat;
import org.mortbay.log.Log;

import com.google.common.collect.Lists;

import difflib.Chunk;
import difflib.Delta;
import edu.umd.cloud9.io.pair.PairOfLongs;
import tuan.hadoop.conf.JobConfig;
import static org.hedera.io.input.WikiRevisionInputFormat.TIME_FORMAT;
import static org.hedera.io.RevisionDiff.opt2byte; 

/**
 * This jobs extract temporal anchor text from Wikipedia revisions. Command line arguments:
 * [INPUTDIR] [OUTPUTDIR] [reduce No]
 * 
 * corresponding to the input directory of the XML revision history dumps files (in HDFS for
 * example), the output directory of extracted text, and the number of concurrent reducers
 * to be run
 * 
 * @author tuan
 *
 */
public class ExtractTemporalAnchorText extends JobConfig implements Tool {

	  public static final String INPUT_OPTION = "input";
	  public static final String OUTPUT_OPTION = "output";
	  public static final String REDUCENO = "reduce";
	
	// Algorithm:
	// emit (id, rev diff) --> ((rev id, timestamp), text)
	private static final class MyMapper extends Mapper<LongWritable, RevisionDiff, 
	PairOfLongs, Text> {

		private PairOfLongs keyOut = new PairOfLongs();
		private Text valOut = new Text();

		// simple counter to sparse the debug printout
		private long cnt;
		
		@Override
		protected void setup(Context context) throws IOException,
		InterruptedException {
			super.setup(context);
			cnt = 0;
		}

		@Override
		protected void map(LongWritable key, RevisionDiff value,
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
							
							// Output anchor in format:
							// [timestamp] TAB [source page ID] TAB [revision ID] TAB [ID of previous revision] TAB [type of modification: CHANGE /DELETE / INSERT] TAB [source page title] TAB [anchor text] TAB [destination title] TAB []
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
							String s = sb.toString();
							valOut.set(s);
							
							// debug hook
							cnt++;
							if (cnt % 1000000l == 0)
								Log.info(s);
							
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

		@Override
		public String toString() {
			return String.format("[target: %s, anchor: %s]", target, anchor);
		}
	}

	@SuppressWarnings("static-access")
	@Override
	public int run(String[] args) throws Exception {
		
		Options options = new Options();

	    options.addOption(OptionBuilder.withArgName("path").hasArg()
	        .withDescription("input path").create(INPUT_OPTION));
	    options.addOption(OptionBuilder.withArgName("path").hasArg()
	        .withDescription("output path").create(OUTPUT_OPTION));
	    options.addOption(OptionBuilder.withArgName("num").hasArg()
	        .withDescription("number of reducer").create(REDUCENO));

	    CommandLine cmdline;
	    CommandLineParser parser = new GnuParser();
	    try {
	      cmdline = parser.parse(options, args);
	    } catch (ParseException exp) {
	      HelpFormatter formatter = new HelpFormatter();
	      formatter.printHelp(this.getClass().getName(), options);
	      ToolRunner.printGenericCommandUsage(System.out);
	      System.err.println("Error parsing command line: " + exp.getMessage());
	      return -1;
	    }
	    
	    File.pathSp

	    if (!cmdline.hasOption(INPUT_OPTION) || !cmdline.hasOption(OUTPUT_OPTION) ||
	        !cmdline.hasOption(REDUCENO)) {
	      HelpFormatter formatter = new HelpFormatter();
	      formatter.printHelp(this.getClass().getName(), options);
	      ToolRunner.printGenericCommandUsage(System.out);
	      return -1;
	    }
		
		String inputDir = cmdline.getOptionValue(INPUT_OPTION);
		String outputDir = cmdline.getOptionValue(OUTPUT_OPTION);
		int reduceNo = Integer.parseInt(cmdline.getOptionValue(REDUCENO));
		
		// this job sucks big memory
		setMapperSize("-Xmx5120m");
		
		// skip non-article
		getConf().setBoolean(WikiRevisionInputFormat.SKIP_NON_ARTICLES, true);
		
		Job job = setup(args[2],ExtractTemporalAnchorText.class, 
				inputDir, outputDir,
				WikiRevisionDiffInputFormat.class, TextOutputFormat.class,
				PairOfLongs.class, Text.class, PairOfLongs.class, Text.class,
				MyMapper.class, Reducer.class, reduceNo);
				
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
