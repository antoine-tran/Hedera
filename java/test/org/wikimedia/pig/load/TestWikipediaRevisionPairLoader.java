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
			while (readUntilMatch(fis)) {
				if (flag == 5) {
					try {
						value.write(pageHeader.toByteArray());
						value.write(rev1Buf.toByteArray(), 0, rev1Buf.size());
						value.write(rev2Buf.toByteArray(), 0, rev2Buf.size());
						value.write(END_PAGE, 0, END_PAGE.length);
					} finally {
						pageHeader.reset();
						rev1Buf.reset();
						rev2Buf.reset();
					}
				} 
				else if (flag == 4 && revisionVisited == 2) {
					value.write(pageHeader.toByteArray());
					value.write(rev1Buf.toByteArray(), 0, rev1Buf.size());
					value.write(rev2Buf.toByteArray(), 0, rev2Buf.size());
					value.write(END_PAGE, 0, END_PAGE.length);
				}
				else if (flag == -1) {
					pageHeader.reset();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println(value);
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
					if (i >= END_REVISION.length) {
						flag = 4;
						revisionVisited++;
						if (revisionVisited == 1) {
							rev1Buf.write(END_REVISION);
						} else if (revisionVisited == 2) {
							rev2Buf.write(END_REVISION);
						} else {
							LOG.error("missing or mal-formed revision tag: " + 
									rev1Buf.toString());
						}
						return true;
					}
				} else i = 0;
				if (revisionVisited == 0) {
					rev1Buf.write(b);
				} else if (revisionVisited == 1) {
					rev2Buf.write(b);
				} else {
					LOG.error("missing or mal-formed revision tag: " + 
							rev1Buf.toString());
				}
			}

			// Note that flag 4 can be the signal of a new record inside one old page
			else if (flag == 4) {
				if (b == END_PAGE[i]) {
					i++;
					if (i >= END_PAGE.length) {
						flag = 5;
						return true;							
					}
				} else if (b == START_REVISION[i]) {
					i++;
					if (i >= START_REVISION.length) {
						flag = 3;
						if (revisionVisited == 0) {
							rev1Buf.write(START_REVISION);
						} else if (revisionVisited == 1) {
							rev2Buf.write(START_REVISION);
						} else {
							LOG.error("missing or mal-formed revision tag: " + 
									rev1Buf.toString());
						}
					}
				} else i = 0;
			} 
		}
	}
}
