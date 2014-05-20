package org.wikimedia.pig.load;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.wikimedia.hadoop.io.WikipediaRevisionInputFormat.RevisionPairRecordReader;

public class TestWikipediaRevisionPairLoader {
	private static final Logger LOG = Logger.getLogger(RevisionPairRecordReader.class); 
	private static final String START_PAGE_TAG = "<page>";
	private static final String END_PAGE_TAG = "</page>";
	private static final byte[] START_PAGE = START_PAGE_TAG.getBytes(StandardCharsets.UTF_8);
	private static final byte[] END_PAGE = END_PAGE_TAG.getBytes(StandardCharsets.UTF_8);
	private static final byte[] START_REVISION = "<revision>".getBytes(StandardCharsets.UTF_8);
	private static final byte[] END_REVISION = "</revision>".getBytes(StandardCharsets.UTF_8);
	private static final byte[] DUMMY_REV = ("<revision beginningofpage=\"true\">"
			+ "<text xml:space=\"preserve\"></text></revision>\n")
			.getBytes(StandardCharsets.UTF_8);
	
	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}
	
	private final StringBuffer value = new StringBuffer();

	@Test
	public void testParser() {
		ByteArrayOutputStream value = new ByteArrayOutputStream();
		try (FileInputStream fis = new FileInputStream("files/testwiki.txt")) {
			flag = 1;
			revisionVisited = 0;
			int matchAfterRevisionEndTag = -1;
			while (readUntilMatch(fis)) {
				if (flag == 5) {
					System.out.println(value);
					value.reset();
					pageHeader.reset();
					rev1Buf.reset();
					rev2Buf.reset();
					revisionVisited = 0;
				} 
				else if (flag == 4) {
					value.write(pageHeader.toByteArray(), 0, pageHeader.size() - 10);
					value.write(rev1Buf.toByteArray());
					value.write(rev2Buf.toByteArray());
					value.write(END_PAGE, 0, END_PAGE.length);
				}
				else if (flag == 2) {
					pageHeader.write(START_PAGE);
				}
				else if (flag == 3) {
					rev1Buf.reset();
					if (revisionVisited == 0) {
						rev1Buf.write(DUMMY_REV);
					} else {
						rev1Buf.write(rev2Buf.toByteArray());
					}
					rev2Buf.reset();
					rev2Buf.write(START_REVISION);
				}
				else if (flag == -1) {
					pageHeader.reset();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// A flag that tells in which block the cursor is:
	// -1: EOF
	// 1 - outside the <page> tag
	// 2 - just passed the <page> tag but outside the <revision>
	// 3 - just passed the (next) <revision>
	// 4 - just passed the </revision>
	// 5 - just passed the </page>
	private byte flag;

	// indicating how many <revision> tags have been met, reset after every record
	private int revisionVisited;
	
	private ByteArrayOutputStream pageHeader = new ByteArrayOutputStream();
	private ByteArrayOutputStream rev1Buf = new ByteArrayOutputStream();
	private ByteArrayOutputStream rev2Buf = new ByteArrayOutputStream();

	private boolean readUntilMatch(FileInputStream fsin) throws IOException {
        int i = 0;
		while (true) {
			int b = fsin.read();
			if (b == -1) {
				flag = -1;
				return false;
			}

			// ignore every character until reaching a new page
			if (flag == 1 || flag == 5) {
				if (b == START_PAGE[i]) {
					i++;
					if (i >= START_PAGE.length) {
						flag = 2;
						return true;
					}
				} else i = 0;
			}

			// put everything between <page> tag and the first <revision> tag into pageHeader
			else if (flag == 2) {
				if (b == START_REVISION[i]) {
					i++;
				} else i = 0;
				pageHeader.write(b);
				if (i >= START_REVISION.length) {
					flag = 3;
					return true;
				}
			}

			// inside <revision></revision> block
			else if (flag == 3) {
				if (b == END_REVISION[i]) {
					i++;
				} else i = 0;
				rev2Buf.write(b);
				if (i >= END_REVISION.length) {
					flag = 4;
					revisionVisited++;
					return true;
				}
			}

			// Note that flag 4 can be the signal of a new record inside one old page
			else if (flag == 4) {
				if (b == END_PAGE[i] || b == START_REVISION[i]) {
					i++;
					if (b == END_PAGE[i] && i >= END_PAGE.length) {
						flag = 5;
						return true;							
					} else if (b ==  START_REVISION[i] && i >= START_REVISION.length) {
						flag = 3;
						return true;
					}
				} else i = 0;
			} 
		}
	}
}
