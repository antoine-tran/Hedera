/**
 * 
 */
package org.hedera.mapreduce;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.hedera.io.SplitUnit;
import org.hedera.io.input.WholeFileRecordReader;
import org.wikimedia.wikihadoop.ByteMatcher;
import org.wikimedia.wikihadoop.SeekableInputStream;

import tuan.hadoop.conf.JobConfig;
import tuan.hadoop.conf.JobConfig.Version;

import static org.hedera.io.input.WikiRevisionInputFormat.END_PAGE_TAG;

/**
 * This tool parses the list of dump files for Wikipedia revision
 * and performs the splitting, then repacks the splits into a
 * sequence file.
 * 
 * The tool accepts two kinds of input: A glob-like string
 * specifying the input path, or a text file containing the list of 
 * file paths, one per line (in this case the flag input-type-option must be turned
 * on)
 * 
 * @author tuan
 *
 */
public class IndexSplits extends JobConfig implements Tool {

	/** I keep the parameter name to honour Matsubara */
	protected static final String KEY_SKIP_FACTOR = 
			"org.wikimedia.wikihadoop.skipFactor";

	private static final Logger LOG = Logger.getLogger(IndexSplits.class);

	public static final String INPUT_OPTION = "input";
	public static final String OUTPUT_OPTION = "output";
	public static final String SPLIT_OPTION = "split_size";

	// When turned on, then the input path is of type .csv
	public static final String INPUT_TYPE_OPTION = "file";
	
	public static final String HADOOP_SPLIT_OPTION = "file.split.size";

	// The mapper performs block size computation and splitting of code,
	// as inherited from Yusuke Matsubara's StreamWikiDumpInputFormat getSplits()
	// method
	private static final class MyMapper extends Mapper<Text, NullWritable,
			SplitUnit, NullWritable> {

		private final NullWritable nullObj = NullWritable.get();
		private CompressionCodecFactory compressionCodecs = null;
		private int splitSize;

		@Override
		protected void setup(Context context) throws IOException,
		InterruptedException {
			Configuration conf = context.getConfiguration();
			compressionCodecs = new CompressionCodecFactory(conf);
			splitSize = conf.getInt(HADOOP_SPLIT_OPTION, 317);
		}

		@Override
		/** each key fed to the mapper is the path of a file
		 * each mapper generates a list of SplitUnits and emits
		 * each of them to the reducer
		 */
		protected void map(Text key, NullWritable value, Context context)
				throws IOException, InterruptedException {
			FileSplit fileSplit = (FileSplit)context.getInputSplit();
			//String s = FileSystem.getF
			Configuration conf = context.getConfiguration();
			Path file = new Path(s);
			FileSystem fs = file.getFileSystem(conf);
			FileStatus[] statuses = fs.listStatus(file);
			FileStatus status = null;
			if (statuses == null || statuses.length > 1) {
				throw new IOException("invalid file: " + s);
			} else {
				status = statuses[0];
				if (status.isDirectory()) {
					throw new IOException("Cannot split the directory: " + s);
				}
			}
			long length = status.getLen();
			List<SplitUnit> units = new LinkedList<>();

			// if the file is non-splittably compressed, simple return itself
			// as the single FileSplit
			CompressionCodec codec = compressionCodecs.getCodec(file);		
			BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);
			if (length == 0 || codec == null || 
					!(codec instanceof SplittableCompressionCodec)) {
				String[] hosts = (length != 0) ?
						blkLocations[blkLocations.length-1].getHosts():
							new String[0];
						units.add(makeSplit(s, 0, length, hosts));
			} 

			// perform the Matsubara splitting code
			else {
				long bytesRemaining = length;
				SplitUnit unit = null;

				SeekableInputStream in = SeekableInputStream.getInstance(file,
						0, length, fs, this.compressionCodecs);
				SplitCompressionInputStream is = in.getSplitCompressionInputStream();
				long start = 0;
				long skip = 0;
				if ( is != null ) {
					start = is.getAdjustedStart();
					length = is.getAdjustedEnd();
					is.close();
					in = null;
				}
				Set<Long> processedPageEnds = new HashSet<Long>();
				float factor = conf.getFloat(KEY_SKIP_FACTOR, 1.2F);

				READLOOP:
					while (((double) bytesRemaining)/splitSize > factor  &&  bytesRemaining > 0) {
						// prepare matcher
						ByteMatcher matcher;
						{
							long st = Math.min(start + skip + splitSize, length - 1);
							unit = makeSplit(s, st, Math.min(splitSize, length - st), 
									blkLocations);
							if ( in != null )
								in.close();
							if ( unit.getLength() <= 1 ) {
								break;
							}
							in = SeekableInputStream.getInstance(unit.fileSplit(), 
									fs, this.compressionCodecs);
						}
						matcher = new ByteMatcher(in);

						// read until the next page end in the look-ahead split
						while ( !matcher.readUntilMatch(END_PAGE_TAG, null, unit.getStart() 
								+ unit.getLength()) ) {
							if (matcher.getPos() >= length  ||  unit.getLength() == length 
									- unit.getStart())
								break READLOOP;
							unit = makeSplit(s,
									unit.getStart(),
									Math.min(unit.getLength() + splitSize, length 
											- unit.getStart()), blkLocations);
						}
						if ( matcher.getLastUnmatchPos() > 0
								&&  matcher.getPos() > matcher.getLastUnmatchPos()
								&&  !processedPageEnds.contains(matcher.getPos()) ) {
							units.add(makeSplit(s, start, matcher.getPos() - start, 
									blkLocations));
							processedPageEnds.add(matcher.getPos());
							long newstart = Math.max(matcher.getLastUnmatchPos(), start);
							bytesRemaining = length - newstart;
							start = newstart;
							skip = 0;
						} else {
							skip = matcher.getPos() - start;
						}
					}

				if (bytesRemaining > 0 && !processedPageEnds.contains(length)) {
					units.add(makeSplit(s, length-bytesRemaining, bytesRemaining, 
							blkLocations[blkLocations.length-1].getHosts()));
				}
				if ( in != null)
					in.close();
			}

			// emit the split info
			for (SplitUnit split : units) {
				context.write(split, nullObj);
			}
		}	

		public SplitUnit makeSplit(String file, long start, long length, 
				BlockLocation[] blkLocations) throws IOException {
			String[] hosts = blkLocations[blkLocations.length-1].getHosts();
			return makeSplit(file, start, length, hosts);
		}

		public SplitUnit makeSplit(String file, long start, long length, 
				String[] hosts) throws IOException {
			return new SplitUnit(file, start, length, hosts);
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
				.withDescription("number of terms").create(SPLIT_OPTION));

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

		if (!cmdline.hasOption(INPUT_OPTION) || !cmdline.hasOption(OUTPUT_OPTION) ||
				!cmdline.hasOption(SPLIT_OPTION)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(this.getClass().getName(), options);
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}

		String input = cmdline.getOptionValue(INPUT_OPTION);
		String output = cmdline.getOptionValue(OUTPUT_OPTION);

		int optSize = 317;
		if (cmdline.hasOption(SPLIT_OPTION)) {
			String s = cmdline.getOptionValue(SPLIT_OPTION);
			try {
				optSize = Integer.parseInt(s);				
			} catch (NumberFormatException e) {
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp(this.getClass().getName(), options);
				ToolRunner.printGenericCommandUsage(System.out);
				LOG.error("invalid split-size option value: " + s);
				return -1;
			}
		}

		Configuration conf = getConf();
		conf.setInt(HADOOP_SPLIT_OPTION, optSize);

		LOG.info("Tool name: " + BuildDictionary.class.getSimpleName());
		LOG.info(" - input: " + input);
		LOG.info(" - output: " + output);

		// : register job here
		// Hadoop 2.0
		Job job = create("Create the split for ", IndexSplits.class);
		
		return 0;
	}
}
