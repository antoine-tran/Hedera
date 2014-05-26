/**
 * This is inspired by the original version of Yusuke Matsubara's StreamWikiDumpInputFormat
 * in wikihadoop project, using the newer Hadoop Mapreduce API. I therefore copied over
 * here all comments, copyright notes etc.
 * 
 * Copyright 2011-2014 Yusuke Matsubara
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hedera.io.input;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/** A InputFormat implementation that splits a Wikipedia Revision File into page fragments, output 
 * them as input records.
 *
 * @author Tuan
 * 
 * @since 13.04.2014
 * 
 * @deprecated The getSplits() method here is not optimized by file system blocks. I re-used Yusuke Matsubara's 
 * getSplits() method. See WikipediaRevisionInputFormat for the optimized method
 *
 */
public class WikipediaRevisionInputFormatOld extends TextInputFormat {
	public static final String START_TAG_KEY = "xmlinput.start";
	public static final String END_TAG_KEY = "xmlinput.end";

	public static final String RECORD_READER_OPT = "recordreader";
	public static final String TIME_SCALE_OPT = "timescale";
	/*public static enum TimeScale {
		HOUR("hour"),
		DAY("day"),
		WEEK("week"),
		MONTH("month");		
		private final String val;		
		private TimeScale(String v) {val = v;}		
		public String toString() {
			return val;
		}
		
		public boolean equalsName(String name) {
			return (val.equals(name));
		}
	}*/
	private static Options opts = new Options();	
	private static final GnuParser parser = new GnuParser();
	private CommandLine options;
	
	public static final String START_PAGE_TAG = "<page>";
	public static final String END_PAGE_TAG = "</page>";
	
	public static final byte[] START_PAGE = START_PAGE_TAG.getBytes(StandardCharsets.UTF_8);
	public static final byte[] END_PAGE = END_PAGE_TAG.getBytes(StandardCharsets.UTF_8);
	
	public static final byte[] START_REVISION = "<revision>".getBytes(StandardCharsets.UTF_8);
	public static final byte[] END_REVISION = "</revision>".getBytes(StandardCharsets.UTF_8);
	
	public static final byte[] START_ID = "<id>".getBytes(StandardCharsets.UTF_8);
	public static final byte[] END_ID = "</id>".getBytes(StandardCharsets.UTF_8);

	protected static long DEFAULT_MAX_BLOCK_SIZE = 134217728l;
	protected static long THRESHOLD = 137438953472l;

	public WikipediaRevisionInputFormatOld() {
		super();
	}

	public WikipediaRevisionInputFormatOld(String optString) {
		super();
		initOptions();
		if (optString != null && !optString.isEmpty()) {
			try {
				options = parser.parse(opts, optString.split(" "));
			} catch (org.apache.commons.cli.ParseException e) {
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp("[-" + RECORD_READER_OPT + "] [-" + TIME_SCALE_OPT + "]", opts);
				throw new RuntimeException(e);
			}	
		}
	}

	private static void initOptions() {
		opts.addOption(RECORD_READER_OPT, true, "The driver class for record reader");
		opts.addOption(TIME_SCALE_OPT, true, "The time scale used to coalesce the timeline");
	}

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, 
			TaskAttemptContext context) {
		Configuration conf = context.getConfiguration();

		// Tu should have done this already (??): Set maximum splitsize to be 128MB
		conf.setLong("mapreduce.input.fileinputformat.split.maxsize", DEFAULT_MAX_BLOCK_SIZE);

		if (options != null) {
			String recordReader = options.getOptionValue(RECORD_READER_OPT);
			if (recordReader == null || recordReader.equalsIgnoreCase("RevisionPair")) {
				return new WikiRevisionAllPairReader();
			} else if (recordReader.equalsIgnoreCase("Revision")) {
				return new WikiRevisionPlainTextReader();
			} else if (recordReader.equalsIgnoreCase("RevisionDistant")) {
				if (!options.hasOption(TIME_SCALE_OPT)) {
					throw new RuntimeException("Must specify the time scale for RevisionDistant");
				} else {
					String scale = options.getOptionValue(TIME_SCALE_OPT);
					/*TimeScale ts = null;
					for (TimeScale t : TimeScale.values()) {
						if (t.equalsName(scale)) {
							ts = t;
						}
					}
					return new WikiRevisionSamplePairReader(ts);*/
					
					// This is just for the compilation. Never use this driver again
					return null;
				}
			} else throw new RuntimeException("unknown recorder driver");
		} else return new WikiRevisionAllPairReader();
	}

	@Override
	public boolean isSplitable(JobContext context, Path file) {
		Configuration conf = context.getConfiguration();
		CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(conf);
		CompressionCodec codec = compressionCodecs.getCodec(file);		
		if (null == codec) {
			return true;
		}
		return codec instanceof SplittableCompressionCodec;
	}

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException {
		List<InputSplit> splits = new ArrayList<>();
		for (FileStatus fs : listStatus(context)) {
			Path f = fs.getPath();
			Configuration conf = context.getConfiguration();
			FileSystem fsys = f.getFileSystem(conf);
			if (isSplitable(context, fs.getPath())) {
				splits.addAll(getSplitsForXMLTags(fs, conf, THRESHOLD));
			} else {
				long length = fs.getLen();
				BlockLocation[] blkLocs = fsys.getFileBlockLocations(fs, 0, length);
				if (length != 0) {
					splits.add(new FileSplit(f, 0, length, blkLocs[0].getHosts()));
				} else {
					splits.add(new FileSplit(f, 0, length, new String[]{}));
				}
			}
		}
		return splits;
	}

	// Splits a (possibly compressed) xml files by <page></page> chunks, with respect to the 
	// maximum size of one file
	protected List<FileSplit> getSplitsForXMLTags(FileStatus status, Configuration conf, 
			long threshold) throws IOException {

		List<FileSplit> splits = new ArrayList<>();
		Path file = status.getPath();
		if (status.isDirectory()) {
			throw new IOException("Not a file: " + file);
		}

		CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(conf);
		CompressionCodec codec = compressionCodecs.getCodec(file);
		FileSystem fs = file.getFileSystem(conf);
		InputStream fsin = null;

		try {
			if (codec != null) { // file is compressed
				fsin = codec.createInputStream(fs.open(file));
			} else { // file is uncompressed	
				FSDataInputStream stream = fs.open(file);
				stream.seek(0);
				fsin = stream;
			}

			long start = -1l;
			long[] pos = new long[3];
			byte[] buf = new byte[134217728];
			while (true) {
				if (readUntilMatch(fsin, START_PAGE, pos, buf)) {
					if (start < 0)
						start = pos[0] - START_PAGE.length;
					if (readUntilMatch(fsin, END_PAGE, pos, buf)) {
						if (pos[0] - start >= threshold) {
							BlockLocation[] blkLocs = fs.getFileBlockLocations(status, start, 
									pos[0] - start);
							int blkIndex = getBlockIndex(blkLocs, start);
							splits.add(new FileSplit(file, start, pos[0] - start, 
									blkLocs[blkIndex].getHosts()));
							start = -1;
						}
					} else break;
				} else break;
			}
			/*if (start < pos[0]) {
				splits.add(new FileSplit(file, start, pos[0] - start, new String[]{}));
			}*/
		} finally {
			if (fsin != null) fsin.close();
		}
		return splits;
	}

	private static boolean readUntilMatch(InputStream in, byte[] match, long[] pos, byte[] buf) 
			throws IOException {
		int i = 0;
		while (true) {
			if (pos[1] == pos[2]) {
				pos[2] = in.read(buf);
				pos[1] = 0;
				if (pos[2] == -1) {
					return false;
				}
			} 
			while (pos[1] < pos[2]) {
				byte b = buf[(int) pos[1]];
				pos[0]++;
				pos[1]++;
				if (b == match[i]) {
					i++;
					if (i >= match.length) {
						return true;
					}
				} else i = 0;
			}
		}
	}
}