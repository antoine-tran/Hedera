package org.hedera.io.etl;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

import com.twitter.elephantbird.util.TaskHeartbeatThread;

import static org.hedera.io.input.WikiRevisionInputFormat.START_PAGE;
import static org.hedera.io.input.WikiRevisionInputFormat.END_PAGE;
import static org.hedera.io.input.WikiRevisionInputFormat.START_REVISION;
import static org.hedera.io.input.WikiRevisionInputFormat.END_TEXT;

public abstract class WikiRevisionETLReader<KEYIN, VALUEIN, META> 
		extends RecordReader<KEYIN, VALUEIN> {

	private static final Logger LOG = Logger.getLogger(WikiRevisionETLReader.class);
	
	protected static long DEFAULT_MAX_BLOCK_SIZE = 134217728l;

	private static final float DEFAULT_LOWER_THRESHOLD = 0.01f;
	private static final float DEFAULT_UPPER_THRESHOLD = 0.1f;

	/** The acknowledgement signal when invoking one internal consuming method.
	 * There are three states can return:
	 * - PASSED_TO_NEXT_TAG: the consumer succeeds and now passed the next tag
	 * - EOF: the consumer doesn't encounter the desired tag and it reaches
	 * the file EOF byte
	 * - SKIPPED: The consumter doesn't reach the desired tag yet, but
	 * it will skip to the end of the page
	 * - FAILED: The consumer fails due to internal errors 
	 */
	public static enum Ack {
		PASSED_TO_NEXT_TAG,
		EOF,
		SKIPPED,
		FAILED
	}

	// mark the cursor of the input stream
	private long start;
	private long end;

	// A flag that tells in which block the cursor is.
	// Generic setting:
	// -1: EOF
	// 1: Before the first page
	// 2: Inside the page, does not reach the end revision yet
	// 3: outside the page block
	// 4: The boundary case - The last and second last revisions are 
	// both worth extracting for information
	private byte flag;

	// compression mode checking
	private boolean compressed = false;

	// a direct buffer to improve the local IO performance
	private byte[] buf = new byte[134217728];
	private int[] pos = new int[2];

	private Seekable fsin;

	private KEYIN key;
	private VALUEIN value;

	// caches for the last established revision
	private META meta;
	private DataOutputBuffer prevBuf = new DataOutputBuffer();

	// cache for the currently visited revision	
	private DataOutputBuffer curBuf = new DataOutputBuffer();
	private META curMeta;

	protected ETLExtractor<KEYIN, VALUEIN, META> extractor;

	protected abstract META initializeMeta();

	protected abstract ETLExtractor<KEYIN, VALUEIN, META> initializeExtractor();

	private TaskAttemptContext context;
	
	@Override
	public KEYIN getCurrentKey() throws IOException, InterruptedException {
		return key;
	}	
	protected abstract KEYIN initializeKey();

	protected abstract void freeKey(KEYIN key);

	@Override
	public VALUEIN getCurrentValue() throws IOException, InterruptedException {
		return value;
	}
	protected abstract VALUEIN initializeValue();

	protected abstract void freeValue(VALUEIN value);

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return (fsin.getPos() - start) / (float) (end - start);
	}
	
	protected TaskAttemptContext getTaskAttemptContext() {
		return context;
	}

	@Override
	/**
	 * Each ETLReader must set the key, value Mapper input as well as specify
	 * the extractor, and instantiate the meta object (curMeta)
	 */
	public void initialize(InputSplit input, TaskAttemptContext tac)
			throws IOException, InterruptedException {		

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
		meta = null;
		initializeObjects();
		this.context = tac;
	}

	private void initializeObjects() {
		key = initializeKey();
		value = initializeValue();
		curMeta = initializeMeta();
		extractor = initializeExtractor();
	}	

	protected static void setBlockSize(Configuration conf) {
		conf.setLong("mapreduce.input.fileinputformat.split.maxsize", 
				DEFAULT_MAX_BLOCK_SIZE);
	}

	private void updateRevision() throws IOException {
		meta = curMeta;
		prevBuf.reset();

		// some ETL Reader dont read the content at all !!
		if (curBuf.getLength() > 0) {
			prevBuf.write(curBuf.getData(), 0, curBuf.getLength() 
					- END_TEXT.length);
			curBuf.reset();
		}
	}

	private void clearRevisions() {
		meta = null;
		prevBuf.reset();
		curBuf.reset();
		freeKey(key);
		freeValue(value);
	}

	@Override
	//
	// Tuan: This is one of the most error-prone, tedious code I've ever written :(
	// I feel like I have to write the documentation for this method somewhere. Otherwise
	// I will lose understanding it next few months
	//
	public boolean nextKeyValue() throws IOException, InterruptedException {
		while (fsin.getPos() < end) {

			if (flag == -1) {
				return false;
			}

			// the rare case: One last revision from last page still needs
			// to be processed
			if (flag == 4) {
				freeKey(key);
				freeValue(value);
				extractor.extract(prevBuf, meta, key, value);
				flag = 3;
				return true;
			}
			else if (flag == 1 || flag == 3) {

				while (hasNextPage()) {

					// before we start, let's clean all buffers
					clearRevisions();

					Ack r = readToPageHeader(curMeta);
					if (r == Ack.EOF) 
						return false;
					else if (r == Ack.FAILED) 
						throw new IOException("error when reading the next "
								+ "<revision>");			
					// Next_Tag = Revision in this case
					else if (r == Ack.PASSED_TO_NEXT_TAG) {
						flag = 2;
						break;
					}
					else continue;
				}
			}
			if (flag == 2) {				
				Ack r = readToNextRevision(curBuf, curMeta);
				if (r == Ack.EOF)
					return false;
				else if (r == Ack.FAILED)
					throw new IOException("error when reading the next "
							+ "</revision");

				// We never have skipped inside the revision block

				else if (r == Ack.PASSED_TO_NEXT_TAG) {

					// The first revision always replace the previous (empty) one
					if (meta == null) {						
						updateRevision();
						if (hasNextRevision()) {
							continue;
						} 

						// the last revision, extract and stop
						else {
							flag = 3;
							freeKey(key);
							freeValue(value);
							extractor.extract(prevBuf,meta,key,value);
							return true;
						}
					}

					// heuristics: 
					// - If the two revisions are too similar (< 0.01), throw 
					// away the previous revision and get the new one and 
					// continue.
					// - If the two revisions are different enough (> 0.1), 
					// perform the extraction on the previous revision, making
					// sure the value is clean when seeing the revisions 
					// independently. Then throw it away, get the new rev and
					// stop.
					else {
						float score = extractor.check(curMeta, meta);
						if (score < DEFAULT_LOWER_THRESHOLD) {
							updateRevision();
							if (hasNextRevision()) {
								continue;
							} 

							// the last revision, extract and stop
							else {
								flag = 3;
								freeKey(key);
								freeValue(value);
								extractor.extract(prevBuf,meta,key,value);
								return true;
							}
						}
						else if (score > DEFAULT_UPPER_THRESHOLD) {
							freeKey(key);
							freeValue(value);
							extractor.extract(prevBuf,meta,key,value);

							// TODO: Tricky scenario: The very last revision just has
							// a big change. 
							if (!hasNextRevision()) {
								// By turning a special flag value, we hope it will not
								// be forgotten the next read
								flag = 4;
							}
							updateRevision();
							return true;
						}						
					}
				}

				else if (r == Ack.SKIPPED) {
					if (hasNextRevision()) {
						continue;
					} 

					// the last revision, extract and stop
					else {
						flag = 3;
						freeKey(key);
						freeValue(value);
						extractor.extract(prevBuf,meta,key,value);
						return true;
					}
				}
			}			
		}
		return false;
	}

	/**
	 * Consume all the tags from page tag till the first revision tag. Cache
	 * the values to meta data if needed
	 * @return true when reaching revision, false when EOF
	 */
	protected abstract Ack readToPageHeader(META meta) throws IOException;

	/**
	 * This method reads bytes inside the input stream into the buffer
	 * until reaching EOF or the revision close tag. In case of success,
	 * it extracts the meta-data into the meta form  
	 */
	protected abstract Ack readToNextRevision(DataOutputBuffer buffer, META meta)
			throws IOException;	

	/**
	 * Outside the <page> block, check if next <page> tag comes
	 * @return true if next page has been found,
	 *         false if the EOF has been found
	 * @throws IOException 
	 */
	private boolean hasNextPage() throws IOException {	
		int i = 0;
		while (true) {
			if (!fetchMore()) return false;
			while (hasData()) {
				byte b = nextByte();				
				if (b == START_PAGE[i]) {
					i++;
					if (i >= START_PAGE.length) {
						return true;
					}
				} else i = 0;
			}
		}		
	}

	/**
	 * Outside the revision block, check for next revision tag of the
	 * page. Return true if next revision found, false if EOF or closing of page
	 * found
	 * @throws IOException 
	 */
	private boolean hasNextRevision() throws IOException {
		int i = 0;
		int revOrPage = -1;
		while (true) {
			if (!fetchMore()) return false;
			while (hasData()) {
				byte b = nextByte();
				int curMatch = 0;				
				if ((i < END_PAGE.length && b == END_PAGE[i]) 
						&& (i < START_REVISION.length && b == START_REVISION[i])) {
					curMatch = 3;
				} else if (i < END_PAGE.length && b == END_PAGE[i]) {
					curMatch = 2;
				} else if (i < START_REVISION.length && b == START_REVISION[i]) {
					curMatch = 1;
				}				
				if (curMatch > 0 && (i == 0 || revOrPage == 3 || curMatch == revOrPage)) {					
					i++;			
					revOrPage = curMatch;
				} else i = 0;
				if ((revOrPage == 2 || revOrPage == 3) && i >= END_PAGE.length) {
					return false;							
				} else if ((revOrPage == 1 || revOrPage == 3) && i >= START_REVISION.length) {
					return true;
				}
			}
		}
	}	

	/** Read the stream and update the internal buffer if necessary. Always return
	 * true except when reaching EOF 
	 * @throws IOException */
	protected final boolean fetchMore() throws IOException {
		if (buf == null && pos.length != 2)
			throw new IOException("Internal buffer corrupted.");
		if (pos[0] == pos[1]) {	
			
			// We use a thread that pings back to the cluster every 5 minutes
			// to avoid getting killed for slow read
			TaskHeartbeatThread hearbeat = new TaskHeartbeatThread(context, 60 * 5000) {
				@Override
				protected void progress() {
					LOG.info("Task " + context.getTaskAttemptID() 
							+ " pings back...");
				}
			};
			
			try {
				hearbeat.start();
				pos[1] = (compressed) ? ((InputStream)fsin).read(buf) :
					((FSDataInputStream)fsin).read(buf);
				pos[0] = 0;
			} finally {
				hearbeat.stop();
			}
			
			if (pos[1] == -1) {
				flag = -1;
				return false;
			}
		} return true; 
	}

	/** Check whether there are still data to read */
	protected boolean hasData() {
		return (pos[0] < pos[1]);
	}

	/** Get the next byte in the stream and move the cursor forward */
	protected byte nextByte() {
		byte b = buf[pos[0]];
		pos[0]++;
		return b;
	}

	@Override
	public void close() throws IOException {
		if (compressed) {
			((CompressionInputStream)fsin).close();
		} else {
			((FSDataInputStream)fsin).close();
		}
	}
}