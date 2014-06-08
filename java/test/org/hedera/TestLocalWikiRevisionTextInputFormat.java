package org.hedera;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Simulate and test WikiRevisionTextInputFormat in local setting
 */
public class TestLocalWikiRevisionTextInputFormat {

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

	protected static long DEFAULT_MAX_BLOCK_SIZE = 134217728l;
	protected static long THRESHOLD = 137438953472l;


	private static final String INPUT = "/home/tuan/Downloads/enwiki-latest-pages-meta-history6.xml-p000236948p000252388";

	private DataOutputBuffer pageHeader = new DataOutputBuffer();

	// We now convert and cache everything from pageHeader to the followin global variables
	// NOTE: they all need to be synchronized with pageHeader !!

	private DataOutputBuffer revBuf = new DataOutputBuffer();
	private DataOutputBuffer keyBuf = new DataOutputBuffer();

	private final LongWritable key = new LongWritable();
	private final Text value = new Text();

	private byte[] buf = new byte[134217728];
	private int[] pos = new int[2];

	FileInputStream fis;

	private int flag;
	private int lastMatchTag = -1;
	

	@Before
	public void initialize() throws FileNotFoundException {
		fis = new FileInputStream(INPUT);
		flag = 1;

	}

	public boolean nextKeyValue() throws NumberFormatException, IOException {
		while (readUntilMatch()) {
			if (flag == 7) {
				pageHeader.reset();
				value.clear();
			} 
			else if (flag == 6) {					
				value.set(pageHeader.getData(), 0, pageHeader.getLength() 
						- START_REVISION.length);

				value.append(revBuf.getData(), 0, revBuf.getLength());
				value.append(END_PAGE, 0, END_PAGE.length);
				revBuf.reset();
				return true;
			}
			else if (flag == 4) {
				String pageId = new String(keyBuf.getData(), 0, keyBuf.getLength()
						- END_ID.length);
				key.set(Long.parseLong(pageId));	
				keyBuf.reset();
			}
			else if (flag == 2) {
				pageHeader.write(START_PAGE);
			}
			else if (flag == 5) {
				revBuf.write(START_REVISION);
			}
			else if (flag == -1) {
				pageHeader.reset();
			}
		}
		return false;
	}

	// Scan the tags in SAX manner. Return at every legit tag and inform the program via the global flag
	// Flush into the caches if necessary
	private boolean readUntilMatch() throws IOException {
		if (buf == null && pos.length != 2)
			throw new IOException("Internal buffer corrupted.");
		int i = 0;
		while (true) {
			if (pos[0] == pos[1]) {				
				pos[1] = fis.read(buf);

				pos[0] = 0;
				if (pos[1] == -1) {
					return false;
				}
			} 
			while (pos[0] < pos[1]) {
				byte b = buf[pos[0]];
				pos[0]++;
				// ignore every character until reaching a new page
				if (flag == 1 || flag == 7) {
					if (b == START_PAGE[i]) {
						i++;
						if (i >= START_PAGE.length) {
							flag = 2;
							return true;
						}
					} else i = 0;
				}

				// put everything between <page> tag and the first <id> tag into pageHeader
				else if (flag == 2) {
					if (b == START_ID[i]) {
						i++;
					} else i = 0;
					pageHeader.write(b);
					if (i >= START_ID.length) {
						flag = 3;
						return true;
					}
				}

				// put everything in <id></id> block into pageHeader and keyBuf
				else if (flag == 3) {
					if (b == END_ID[i]) {
						i++;
					} else i = 0;
					pageHeader.write(b);
					keyBuf.write(b);
					if (i >= END_ID.length) {
						flag = 4;
						return true;
					}
				}

				// put everything between </id> tag and the first <revision> tag into pageHeader
				else if (flag == 4) {
					if (b == START_REVISION[i]) {
						i++;
					} else i = 0;
					pageHeader.write(b);
					if (i >= START_REVISION.length) {
						flag = 5;
						return true;
					}
				}

				// inside <revision></revision> block
				else if (flag == 5) {
					if (b == END_REVISION[i]) {
						i++;
					} else i = 0;
					revBuf.write(b);
					if (i >= END_REVISION.length) {
						flag = 6;
						return true;
					}
				}

				// Note that flag 4 can be the signal of a new record inside one old page
				else if (flag == 6) {
					int curMatch = 0;				
					if ((i < END_PAGE.length && b == END_PAGE[i]) 
							&& (i < START_REVISION.length && b == START_REVISION[i])) {
						curMatch = 3;
					} else if (i < END_PAGE.length && b == END_PAGE[i]) {
						curMatch = 2;
					} else if (i < START_REVISION.length && b == START_REVISION[i]) {
						curMatch = 1;
					}				
					if (curMatch > 0 && (i == 0 || lastMatchTag == 3 || curMatch == lastMatchTag)) {					
						i++;			
						lastMatchTag = curMatch;
					} else i = 0;
					if ((lastMatchTag == 2 || lastMatchTag == 3) && i >= END_PAGE.length) {
						flag = 7;
						lastMatchTag = -1;
						return true;							
					} else if ((lastMatchTag == 1 || lastMatchTag == 3) && i >= START_REVISION.length) {
						flag = 5;
						lastMatchTag = -1;
						return true;
					}				
				} 
			}		
		}
	}

	public LongWritable currentKey() {
		return key;
	}
	
	public Text currentValue() {
		return value;
	}

	@After
	public void finish() throws IOException {
		fis.close();
	}
	
	@Test
	public void main() throws IOException {
		while (nextKeyValue()) {
			LongWritable k = currentKey();
			Text content = currentValue();
			System.out.println(k.get());
			System.out.println(content.toString());
		}
	}
}
