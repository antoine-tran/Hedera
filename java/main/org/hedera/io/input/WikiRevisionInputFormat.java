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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.net.NetworkTopology;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.wikimedia.wikihadoop.ByteMatcher;
import org.wikimedia.wikihadoop.SeekableInputStream;

/** A InputFormat implementation that splits a Wikipedia Revision File into page fragments, output 
 * them as input records.
 *
 * @author Tuan
 * @author Matsubara 
 * 
 * @since 13.04.2014
 *
 */
public abstract class WikiRevisionInputFormat<T> extends FileInputFormat<LongWritable, T> {
	protected static final String KEY_SKIP_FACTOR = "org.wikimedia.wikihadoop.skipFactor";
	
	protected CompressionCodecFactory compressionCodecs = null;

	public static final String START_PAGE_TAG = "<page>";
	public static final String END_PAGE_TAG = "</page>";

	public static final byte[] START_PAGE = START_PAGE_TAG.getBytes(StandardCharsets.UTF_8);
	public static final byte[] END_PAGE = END_PAGE_TAG.getBytes(StandardCharsets.UTF_8);

	public static final byte[] START_REVISION = "<revision>".getBytes(StandardCharsets.UTF_8);
	public static final byte[] END_REVISION = "</revision>".getBytes(StandardCharsets.UTF_8);

	public static final byte[] START_ID = "<id>".getBytes(StandardCharsets.UTF_8);
	public static final byte[] END_ID = "</id>".getBytes(StandardCharsets.UTF_8);

	public static final byte[] START_TITLE = "<title>".getBytes(StandardCharsets.UTF_8);
	public static final byte[] END_TITLE = "</title>".getBytes(StandardCharsets.UTF_8);
	
	public static final String START_TIMESTAMP_TAG = "<timestamp>";
	public static final String END_TIMESTAMP_TAG = "</timestamp>";
	public static final byte[] START_TIMESTAMP = START_TIMESTAMP_TAG.getBytes(StandardCharsets.UTF_8);
	public static final byte[] END_TIMESTAMP = END_TIMESTAMP_TAG.getBytes(StandardCharsets.UTF_8);
	
	public static final byte[] START_TEXT = "<text xml:space=\"preserve\">"
			.getBytes(StandardCharsets.UTF_8);
	public static final byte[] END_TEXT = "</text>".getBytes(StandardCharsets.UTF_8);
	
	public static final byte[] START_PARENT_ID = "<text xml:space=\"preserve\">"
			.getBytes(StandardCharsets.UTF_8);
	public static final byte[] END_PARENT_ID = "</text>".getBytes(StandardCharsets.UTF_8);	
	
	public static final DateTimeFormatter TIME_FORMAT = ISODateTimeFormat.dateTimeNoMillis();
	
	protected static long DEFAULT_MAX_BLOCK_SIZE = 134217728l;
	protected static long THRESHOLD = 137438953472l;

	public WikiRevisionInputFormat() {
		super();
	}

	@Override
	public abstract RecordReader<LongWritable, T> createRecordReader(InputSplit input,
			TaskAttemptContext context) throws IOException, InterruptedException;
	
	protected static void setBlockSize(Configuration conf) {
		// Tu should have done this already (??): Set maximum splitsize to be 128MB
		conf.setLong("mapreduce.input.fileinputformat.split.maxsize", DEFAULT_MAX_BLOCK_SIZE);
	}
	
	public void configure(Configuration conf) {
		if (compressionCodecs == null)
			compressionCodecs = new CompressionCodecFactory(conf);
	}

	@Override
	public boolean isSplitable(JobContext context, Path file) {
		Configuration conf = context.getConfiguration();
		configure(conf);
		CompressionCodec codec = compressionCodecs.getCodec(file);		
		if (null == codec) {
			return true;
		}
		return codec instanceof SplittableCompressionCodec;
	}

	/** 
	 * This code is copied from StreamWikiDumpNewInputFormat.java by Yusuke Matsubara.
	 * Thanks to Tu Meteora for adjusting the code to the new mapreduce framework
	 * @param job the job context
	 * @throws IOException
	 */
	@Override
	public List<InputSplit> getSplits(JobContext jc) throws IOException {
		List<InputSplit> splits = super.getSplits(jc);
		List<FileStatus> files = listStatus(jc);
		// Save the number of input files for metrics/loadgen
		
		// check we have valid files
		for (FileStatus file: files) {                
			if (file.isDirectory()) {
				throw new IOException("Not a file: "+ file.getPath());
			}
		}
		long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(jc));
		long maxSize = getMaxSplitSize(jc);
		for (FileStatus file: files) {
			if (file.isDirectory()) {
				throw new IOException("Not a file: "+ file.getPath());
			}
			long blockSize = file.getBlockSize();
			long splitSize = computeSplitSize(blockSize, minSize, maxSize);
			for (InputSplit x: getSplits(jc, file, START_PAGE_TAG, splitSize)) 
				splits.add(x);
		}
		return splits;
	}

	/** 
	 * This code is copied from StreamWikiDumpNewInputFormat.java by Yusuke Matsubara.
	 * Thanks to Tu Meteora for adjusting the code to the new mapreduce framework
	 * @param job the job context
	 * @throws IOException
	 */
	public List<InputSplit> getSplits(JobContext jc, FileStatus file, String pattern, 
			long splitSize) throws IOException {

		NetworkTopology clusterMap = new NetworkTopology();
		List<InputSplit> splits = new ArrayList<InputSplit>();
		Path path = file.getPath();
		Configuration conf = jc.getConfiguration();
		configure(conf);

		long length = file.getLen();
		FileSystem fs = file.getPath().getFileSystem(conf);
		BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);
		if ((length != 0) && isSplitable(jc, path)) { 
			long bytesRemaining = length;

			SeekableInputStream in = SeekableInputStream.getInstance(path, 0, length, fs, 
					this.compressionCodecs);
			SplitCompressionInputStream is = in.getSplitCompressionInputStream();
			long start = 0;
			long skip = 0;
			if ( is != null ) {
				start = is.getAdjustedStart();
				length = is.getAdjustedEnd();
				is.close();
				in = null;
			}
			FileSplit split = null;
			Set<Long> processedPageEnds = new HashSet<Long>();
			float factor = conf.getFloat(KEY_SKIP_FACTOR, 1.2F);

			READLOOP:
				while (((double) bytesRemaining)/splitSize > factor  &&  bytesRemaining > 0) {
					// prepare matcher
					ByteMatcher matcher;
					{
						long st = Math.min(start + skip + splitSize, length - 1);
						split = makeSplit(path, st, Math.min(splitSize, length - st), 
								clusterMap, blkLocations);
						if ( in != null )
							in.close();
						if ( split.getLength() <= 1 ) {
							break;
						}
						in = SeekableInputStream.getInstance(split, fs, this.compressionCodecs);
					}
					matcher = new ByteMatcher(in);

					// read until the next page end in the look-ahead split
					while ( !matcher.readUntilMatch(END_PAGE_TAG, null, split.getStart() 
							+ split.getLength()) ) {
						if (matcher.getPos() >= length  ||  split.getLength() == length 
								- split.getStart())
							break READLOOP;
						split = makeSplit(path,
								split.getStart(),
								Math.min(split.getLength() + splitSize, length - split.getStart()),
								clusterMap, blkLocations);
					}
					if ( matcher.getLastUnmatchPos() > 0
							&&  matcher.getPos() > matcher.getLastUnmatchPos()
							&&  !processedPageEnds.contains(matcher.getPos()) ) {
						splits.add(makeSplit(path, start, matcher.getPos() - start, clusterMap, 
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
				splits.add(makeSplit(path, length-bytesRemaining, bytesRemaining, 
						blkLocations[blkLocations.length-1].getHosts()));
			}
			if ( in != null )
				in.close();
		} else if (length != 0) {
			splits.add(makeSplit(path, 0, length, clusterMap, blkLocations));
		} else { 
			//Create empty hosts array for zero length files
			splits.add(makeSplit(path, 0, length, new String[0]));
		}
		return splits;
	}

	private FileSplit makeSplit(Path path, long start, long size, NetworkTopology clusterMap, 
			BlockLocation[] blkLocations) throws IOException {
		String[] hosts = blkLocations[blkLocations.length-1].getHosts();
		return makeSplit(path, start, size,hosts);
	}

	/**
	 * Tuan, Tu (22.05.2014) - For some reasons, the Pig version in the Hadoop@L3S does not 
	 * recognize this method in FileInputFormat. We need to hard-code and copied the source
	 * code over here
	 */
	@Override
	protected FileSplit makeSplit(Path file, long start, long length, String[] hosts) {
		return new FileSplit(file, start, length, hosts);
	}
}