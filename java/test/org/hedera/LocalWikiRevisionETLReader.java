package org.hedera;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.io.DataOutputBuffer;
import org.hedera.io.CloneableObject;
import org.hedera.io.etl.ETLExtractor;


/** The local variant of WikiRevisionETLReader for testing purposes */
public abstract class LocalWikiRevisionETLReader<
		META extends CloneableObject<META>,KEYIN,VALUEIN> {

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

	private static final String INPUT = "files/testwiki.txt";

	private static final float DEFAULT_LOWER_THRESHOLD = 0.01f;
	private static final float DEFAULT_UPPER_THRESHOLD = 0.1f;

	private FileInputStream fis;

	public static enum Ack {
		PASSED_TO_NEXT_TAG,
		EOF,
		SKIPPED,
		FAILED
	}

	private KEYIN key;
	private VALUEIN value;

	// caches for the last established revision
	private META meta;
	private DataOutputBuffer prevBuf = new DataOutputBuffer();

	// cache for the currently visited revision	
	private DataOutputBuffer curBuf = new DataOutputBuffer();
	private META curMeta;

	protected ETLExtractor<KEYIN, VALUEIN, META> extractor;
	
	// A flag that tells in which block the cursor is.
	// Generic setting:
	// -1: EOF
	// 1: Before the first page
	// 2: Inside the page, does not reach the end revision yet
	// 3: outside the page block
	// 4: The boundary case - The last and second last revisions are 
	// both worth extracting for information
	private byte flag;

	// a direct buffer to improve the local IO performance
	private byte[] buf = new byte[134217728];
	private int[] pos = new int[2];
			
	protected abstract META initializeMeta();
	
	protected abstract ETLExtractor<KEYIN, VALUEIN, META> initializeExtractor();
	
	public KEYIN getCurrentKey() throws IOException, InterruptedException {
		return key;
	}	
	
	protected abstract KEYIN initializeKey();
	protected abstract void freeKey(KEYIN key);

	public VALUEIN getCurrentValue() throws IOException, InterruptedException {
		return value;
	}
	
	protected abstract VALUEIN initializeValue();
	protected abstract void freeValue(VALUEIN value);


	/**
	 * Each ETLReader must set the key, value Mapper input as well as specify
	 * the extractor, and instantiate the meta object (curMeta)
	 */
	public void initialize() throws IOException {	
		fis = new FileInputStream(INPUT);
		flag = 1;
		pos[0] = pos[1] = 0;
		meta = null;
		initializeOutput();
	}
	
	private void initializeOutput() {
		key = initializeKey();
		value = initializeValue();
		curMeta = initializeMeta();
		extractor = initializeExtractor();
	}	
	
	private void updateRevision() throws IOException {
		if (meta == null) {
			meta = initializeMeta();
		}
		meta.clone(curMeta);
		prevBuf.reset();
		prevBuf.write(curBuf.getData(), 0, curBuf.getLength() 
				- END_TEXT.length);
		curBuf.reset();
	}
	
	private void clearRevisions() {
		meta = null;
		prevBuf.reset();
		curBuf.reset();		
		freeKey(key);
		freeValue(value);
	}

	//
	// Tuan: This is one of the most error-prone, tedious code I've ever written :(
	//
	public boolean nextKeyValue() throws IOException, InterruptedException {
		while (flag != -1) {
			
			// the rare case: One last revision from last page still needs
			// to be processed
			if (flag == 4) {
				extractor.extract(prevBuf, meta, key, value);
				flag = 3;
				return true;
			}
			else if (flag == 1 || flag == 3) {
				
				while (hasNextPage()) {
					
					// before we start, let's clean all buffers
					clearRevisions();
					
					Ack r = readToPageHeader(curMeta);
					
					// debug hook				
					System.out.println("Header: " + curMeta);

					
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
							extractor.extract(prevBuf,meta,key,value);
							return true;
						}
					}
					
					// heuristics: 
					// - If the two revisions are too similar (< 0.01), throw away
					// the previous revision and get the new one and continue.
					// - If the two revisions are different enough (> 0.1), perform
					// the extraction on the previous revision, then throw it away 
					// and get the new one and stop.
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
								extractor.extract(prevBuf,meta,key,value);
								return true;
							}
						}
						else if (score > DEFAULT_UPPER_THRESHOLD) {
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
			pos[1] =  fis.read(buf);
			pos[0] = 0;
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

	public void close() throws IOException {
		fis.close();
	}
}
