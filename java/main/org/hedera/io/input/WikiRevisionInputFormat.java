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
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
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
import org.apache.log4j.Logger;
import org.hedera.io.RevisionSplits;
import org.hedera.util.ByteMatcher;
import org.hedera.util.SeekableInputStream;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

/** A InputFormat implementation that splits a Wikipedia Revision File into page fragments, output 
 * them as input records.
 *
 * @author Tuan
 * @author Matsubara 
 * 
 * @since 13.04.2014
 * 
 * TODO: 2014-06-24: Implement the parallel splitting in MapReduce style, output:
 * (key=Text,value=Null) --> (key=SplitUnit, value=Null). 
 * SplitUnit contains fileName, start, end, host[]....
 *
 */
public abstract class WikiRevisionInputFormat<KEYIN, VALUEIN>
extends FileInputFormat<KEYIN, VALUEIN> {

	protected static final String KEY_SKIP_FACTOR = "org.wikimedia.wikihadoop.skipFactor";

	// New features since 2014-06-25: Fast split files from pre-computed split index
	public static final String SPLIT_INDEX_OPTION = "index";
	public static final String SPLIT_MAPFILE_LOC = "org.hedera.split.index";

	public static final String SKIP_NON_ARTICLES = "org.hedera.input.onlyarticle"; 
	public static final String SKIP_REDIRECT = "org.hedera.input.noredirects"; 

	public static final String REVISION_BEGIN_TIME = "org.hedera.input.begintime";
	public static final String REVISION_END_TIME = "org.hedera.input.begintime";

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

	public static final byte[] START_NAMESPACE = "<ns>".getBytes(StandardCharsets.UTF_8);
	public static final byte[] END_NAMESPACE = "</ns>".getBytes(StandardCharsets.UTF_8);

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

	public static final byte[] MINOR_TAG = "<minor/>".getBytes(StandardCharsets.UTF_8);

	public static final byte[] START_REDIRECT = "<redirect title=".getBytes(StandardCharsets.UTF_8);
	public static final byte[] END_REDIRECT = "</redirect>".getBytes(StandardCharsets.UTF_8);

	public static final DateTimeFormatter TIME_FORMAT = ISODateTimeFormat.dateTimeNoMillis();

	protected static long THRESHOLD = 137438953472l;

	// An umbrella log for debugging
	protected static final Logger LOG = Logger.getLogger(WikiRevisionInputFormat.class);

	public WikiRevisionInputFormat() {
		super();
	}

	@Override
	public abstract RecordReader<KEYIN, VALUEIN> createRecordReader(InputSplit input,
			TaskAttemptContext context) throws IOException, InterruptedException;

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

		List<FileStatus> files = listStatus(jc);
		List<FileStatus> remainingFiles = new ArrayList<>();
		
		List<InputSplit> splits = new ArrayList<InputSplit>();
		long totalSize = 0;
		
		// New features: Load splits from the index
		// Check the index before performing the split on the physical files
		Configuration conf = jc.getConfiguration();

		String mapFile = conf.get(SPLIT_MAPFILE_LOC);
		MapFile.Reader reader = null;
		Text key = null;
		RevisionSplits val = new RevisionSplits();
		try {
			if (mapFile != null) {
				reader = new MapFile.Reader(new Path(mapFile + "/part-r-00000"), 
						conf);
				key = new Text();
			}

			// check we have valid files
			for (FileStatus file: files) {                
				if (file.isDirectory()) {
					throw new IOException("Not a file: "+ file.getPath());
				}
				
				// if found in the index, load the splits into main memory, otherwise
				// add to remainings for next processing
				if (reader != null) {
					key.set(file.getPath().toString());
					if (reader.seek(key)) {
						reader.get(key, val);
						FileSplit[] spl = val.splits();
						for (FileSplit sp : spl) splits.add(sp);
						continue;
					}
				}
				remainingFiles.add(file);
				totalSize += file.getLen();
			}
			long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(jc));
			
			// 2014-06-06: Tuan _ I have to manually increase the file split size
			// here to cope with Wikipedia Revision .bz2 file - the decompressor
			// takes too long to run
			long goalSize = totalSize / 317;
			
			for (FileStatus file : remainingFiles) {
				long blockSize = file.getBlockSize();
				long splitSize = computeSplitSize(goalSize, minSize, blockSize);
				
				for (InputSplit x: getSplits(jc, file, splitSize)) 
					splits.add(x);
			}
		} finally {
			if (reader != null) reader.close();
		}
		
		return splits;
	}

	/** 
	 * This code is copied from StreamWikiDumpNewInputFormat.java by Yusuke Matsubara.
	 * Thanks to Tu Meteora for adjusting the code to the new mapreduce framework
	 * @param job the job context
	 * @throws IOException
	 */
	public List<InputSplit> getSplits(JobContext jc, FileStatus file, long splitSize) throws IOException {

		List<InputSplit> splits = new ArrayList<InputSplit>();
		Path path = file.getPath();
	
		LOG.info("Splitting file " + path.getName());

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
								blkLocations);
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
							+ split.getLength(), null)) {
						if (matcher.getPos() >= length  ||  split.getLength() == length 
								- split.getStart())
							break READLOOP;
						split = makeSplit(path,
								split.getStart(),
								Math.min(split.getLength() + splitSize, length - split.getStart()),
								blkLocations);
					}
					if ( matcher.getLastUnmatchPos() > 0
							&&  matcher.getPos() > matcher.getLastUnmatchPos()
							&&  !processedPageEnds.contains(matcher.getPos()) ) {
						splits.add(makeSplit(path, start, matcher.getPos() - start,  
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
			splits.add(makeSplit(path, 0, length, blkLocations));
		} else { 
			//Create empty hosts array for zero length files
			splits.add(makeSplit(path, 0, length, new String[0]));
		}
		return splits;
	}

	private FileSplit makeSplit(Path path, long start, long size, 
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