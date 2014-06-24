package org.hedera.io.input;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import static org.hedera.io.input.WikiRevisionInputFormat.SKIP_NON_ARTICLES;

public abstract class WikiRevisionReader<VALUEIN> extends 
		RecordReader<LongWritable, VALUEIN> {
	
	protected static long DEFAULT_MAX_BLOCK_SIZE = 134217728l;
	
	// A state after processing one valid tag
	protected static enum STATE {
		CONTINUE,
		STOP_TRUE,
		STOP_FALSE
	}

	/**
	 * A global variable to maintain the state / position of pointers along the reivision files
	 */

	// mark the cursor of the input stream
	protected long start;
	protected long end;

	// A flag that tells in which block the cursor is. Every implementation should
	// play extremely high attention on maintain the accurateness of the flag value
	protected byte flag;

	// compression mode checking
	protected boolean compressed = false;
	
	// option to whether skip non-article pages
	protected boolean skipNonArticles = false;

	// a direct buffer to improve the local IO performance
	protected byte[] buf = new byte[134217728];
	protected int[] pos = new int[2];

	protected Seekable fsin;

	protected LongWritable key = new LongWritable();
	protected VALUEIN value;
	
	protected DataOutputBuffer keyBuf = new DataOutputBuffer();		
	
	protected boolean skipped = false;

	@Override
	public void initialize(InputSplit input, TaskAttemptContext tac)
			throws IOException, InterruptedException {

		// config xmlinput properties to support bzip2 splitting
		Configuration conf = tac.getConfiguration();
		setBlockSize(conf);

		FileSplit split = (FileSplit) input;
		start = split.getStart();
		end = start + split.getLength();
		Path file = split.getPath();

		CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(conf);
		CompressionCodec codec = compressionCodecs.getCodec(file);

		FileSystem fs = file.getFileSystem(conf);

		if (codec != null) { // file is compressed
			compressed = true;
			// fsin = new FSDataInputStream(codec.createInputStream(fs.open(file)));
			CompressionInputStream cis = codec.createInputStream(fs.open(file));

			cis.skip(start - 1);

			fsin = cis;
		} else { // file is uncompressed	
			compressed = false;
			fsin = fs.open(file);
			fsin.seek(start);
		}
		flag = 1;
		pos[0] = pos[1] = 0;	
		skipNonArticles = conf.getBoolean(SKIP_NON_ARTICLES, true);
	}

	protected static void setBlockSize(Configuration conf) {
		conf.setLong("mapreduce.input.fileinputformat.split.maxsize", 
				DEFAULT_MAX_BLOCK_SIZE);
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return key;
	}
	
	@Override
	public VALUEIN getCurrentValue() 
			throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return (fsin.getPos() - start) / (float) (end - start);
	}

	@Override
	public void close() throws IOException {
		if (compressed) {
			((CompressionInputStream)fsin).close();
		} else {
			((FSDataInputStream)fsin).close();
		}
	}

	@Override	
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (fsin.getPos() < end) {
			while (readUntilMatch()) {
				STATE state = doWhenMatch();
				if (state == STATE.STOP_TRUE) 
					return true;
				else if (state == STATE.STOP_FALSE)
					return false;
			}
		}
		return false;
	}

	/** What to do when encountering one relevant tag */
	protected abstract STATE doWhenMatch() throws IOException, InterruptedException;
	
	/** What to do when reading till the next relevant tag */
	protected abstract boolean readUntilMatch() throws IOException;
}
