package org.hedera.io.input;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.hedera.io.RevisionDiffOld;

import difflib.Delta;
import difflib.DiffUtils;
import difflib.Patch;

public class WikiRevisionDiffInputFormat 
		extends WikiRevisionInputFormat<LongWritable, RevisionDiffOld> {
	
	@Override
	public RecordReader<LongWritable, RevisionDiffOld> createRecordReader(
			InputSplit input, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new DiffReader(); 
	}

	/**
	 * Read every pairs of consecutive revisions and calculate their diffs
	 * using Meyer's alogirthm. Return WikipediaRevisionDiff which, among other fields,
	 * emits the list of diff between the two texts
	 *
	 * @author tuan
	 */
	// States of the flag:
	// 				
	// -1: EOF
	// 1 - outside the <page> tag
	// 2 - just passed the <page> tag but outside the <title>
	// 3 - just passed the <title> tag		
	// 4 - just passed the </title> tag but outside the <namespace>
	// 5 - just passed the <namespace>
	// 6 - just passed the </namespace> but outside the <id>
	// 7 - just passed the (page's) <id>
	// 8 - just passed the </id> tag but outside the <revision>	
	// 9 - just passed the (next) <revision>
	// 10 - just passed the inner <id> tag inside <revision>
	// 11 - just passed the inner </id> tag inside <revision>
	// 12 - just passed the <timestamp>
	// 13 - just passed the </timestamp> tag
	// 14 - just passed the <parentId>
	// 15 - just passed the </parentId> tag
	// 16 - just passed the <text> tag
	// 17 - just passed the </text> tag
	// 18 - just passed the </revision>
	// 19 - just passed the </page>
	public static class DiffReader extends WikiRevisionReader<RevisionDiffOld> {

		// Extra flags:
		//
		// indicating the flow condition within [flag = 16]
		// -1 - Unmatched
		//  1 - Matched <revision> tag partially
		//  2 - Matched </page> tag partially
		//  3 - Matched both <revision> and </page> partially
		private int revOrPage = -1;

		// indicating the flow condition within [flag = 9]
		// -1 - Unmatched
		//  1 - Matched <parentId> tag partially
		//  2 - Matched <timestamp> tag partially
		//  3 - Matched both <parentId> and <timestamp> partially
		private int parOrTs = -1;

		// We now convert and cache everything from pageHeader to the followin global variables
		// NOTE: they all need to be synchronized with pageHeader !!
		// private DataOutputBuffer pageHeader = new DataOutputBuffer();
		private DataOutputBuffer pageTitle = new DataOutputBuffer();
		private DataOutputBuffer nsBuf = new DataOutputBuffer();
		//////////////////////////////////////////////////////////////
		// END PageHeader variables
		//////////////////////////////////////////////////////////////

		// buffer for handling consecutive revisions
		private DataOutputBuffer timestampBuf = new DataOutputBuffer();		
		private DataOutputBuffer revIdBuf = new DataOutputBuffer();		
		private DataOutputBuffer parBuf = new DataOutputBuffer();

		private List<String> lastRevText = new LinkedList<>();
		private DataOutputBuffer contentBuf = new DataOutputBuffer();
		//////////////////////////////////////////////////////////////
		// END revision buffer variables
		//////////////////////////////////////////////////////////////

		@Override
		public void initialize(InputSplit input, TaskAttemptContext tac)
				throws IOException, InterruptedException {
			super.initialize(input, tac);
			value = new RevisionDiffOld(); 
		}

		private void resetEverything() {
			revOrPage = -1;
			parOrTs = -1;
			nsBuf.reset();
			timestampBuf.reset();
			revIdBuf.reset();
			parBuf.reset();
			contentBuf.reset();
			keyBuf.reset();
			pageTitle.reset();
			value.clear();	
			lastRevText.clear();
			skipped = false;
		}

		@Override
		protected STATE doWhenMatch() throws IOException, InterruptedException {
			if (flag == 19) {
				resetEverything();
			}

			// emit the object when reaching </revision>
			else if (flag == 18) {
				if (!skipped)
					return STATE.STOP_TRUE;
			}

			// calculating the diff and shift the revision text when seeing </text>
			// inside the <revision> block
			else if (flag == 17) {
				if (!skipped) {
					// create a mass number of strings
					List<String> content = extractParagraph(contentBuf.getData(), 0,
							contentBuf.getLength() - END_TEXT.length);

					Patch patch = DiffUtils.diff(lastRevText, content);						
					for (Delta d : patch.getDeltas()) {
						value.add(d);
					}						

					lastRevText = content;						
				}
				// release big chunk of bytes here
				contentBuf.reset();
			}

			else if (flag == 15) {
				if (!skipped) {
					String parIdStr = new String(parBuf.getData(), 0, parBuf.getLength() 
							- END_PARENT_ID.length);
					long parId = Long.parseLong(parIdStr);
					value.setParentId(parId);
				}
				parBuf.reset();
			}

			else if (flag == 13) {
				if (!skipped) {
					String ts = new String(timestampBuf.getData(), 0, timestampBuf.getLength() 
							- END_TIMESTAMP.length);
					long timestamp = TIME_FORMAT.parseMillis(ts);
					value.setTimestamp(timestamp);
				}
				timestampBuf.reset();
			}

			else if (flag == 11) {
				if (!skipped) {
					String idStr = new String(revIdBuf.getData(), 0, revIdBuf.getLength()
							- END_ID.length);
					long revId = Long.parseLong(idStr);
					value.setRevisionId(revId);
				}
				revIdBuf.reset();
			}

			else if (flag == 8) {
				if (!skipped) {
					String idStr = new String(keyBuf.getData(), 0, keyBuf.getLength()
							- END_ID.length);
					long pageId = Long.parseLong(idStr);
					key.set(pageId);
					value.setPageId(pageId);
				}
				keyBuf.reset();
			}

			else if (flag == 6) {
				String nsStr = new String(nsBuf.getData(), 0, nsBuf.getLength()
						- END_NAMESPACE.length);
				int ns = Integer.parseInt(nsStr);
				if (ns == 0) {
					skipped = skipNonArticles;					
				}
				value.setNamespace(ns);
				nsBuf.reset();
			}

			else if (flag == 4) {
				String title = new String(pageTitle.getData(), 0, pageTitle.getLength()
						- END_TITLE.length);
				value.setPageTitle(title);
				pageTitle.reset();
			}

			else if (flag == -1) {
				return STATE.STOP_FALSE;
			}
			return STATE.CONTINUE;

		}

		public static List<String> extractParagraph(byte[] b, int offset, int len) 
				throws UnsupportedEncodingException {
			List<String> res = new LinkedList<>();		
			if (b != null && b.length > 0) {
				int start = offset;
				int i = offset;
				while (i < len) {
					char c = (char) (((b[i] & 0xFF) << 8) + (b[i+1] & 0xFF));
					if (c == '\n') {
						String s = new String(b,start,i,"UTF-8");
						res.add(s);

						while (Character.isWhitespace(c)) {
							i += 2;
							c = (char) (((b[i] & 0xFF) << 8) + (b[i+1] & 0xFF));							
						}	
						start = i;
					}
					else {
						i += 2;
					}
				}
				if (start < i) {
					String s = new String(b,start,i,"UTF-8");
					res.add(s);
				}
			}
			return res;
		}

		@Override
		protected boolean readUntilMatch() throws IOException {
			if (buf == null && pos.length != 2)
				throw new IOException("Internal buffer corrupted.");
			int i = 0;
			while (true) {
				if (pos[0] == pos[1]) {				
					pos[1] = (compressed) ? ((CompressionInputStream)fsin).read(buf) :
						((FSDataInputStream)fsin).read(buf);
					pos[0] = 0;
					LOG.info(pos[1] + " bytes read from the stream...");
					if (pos[1] == -1) {
						return false;
					}
				} 
				while (pos[0] < pos[1]) {
					byte b = buf[pos[0]];
					pos[0]++;

					// ignore every character until reaching a new page
					if (flag == 1 || flag == 19) {
						if (b == START_PAGE[i]) {
							i++;
							if (i >= START_PAGE.length) {
								flag = 2;
								return true;
							}
						} else i = 0;
					}

					else if (flag == 2) {
						if (b == START_TITLE[i]) {
							i++;
						} else i = 0;
						if (i >= START_TITLE.length) {
							flag = 3;
							return true;
						}
					}

					// put everything between <title></title> block into title
					else if (flag == 3) {
						if (b == END_TITLE[i]) {
							i++;
						} else i = 0;
						pageTitle.write(b);
						if (i >= END_TITLE.length) {
							flag = 4;
							return true;
						}
					}

					else if (flag == 4) {
						if (b == START_NAMESPACE[i]) {
							i++;
						} else i = 0;
						if (i >= START_NAMESPACE.length) {
							flag = 5;
							return true;
						}
					}

					else if (flag == 5) {
						if (b == END_NAMESPACE[i]) {
							i++;
						} else i = 0;
						nsBuf.write(b);
						if (i >= END_NAMESPACE.length) {
							flag = 6;
							return true;
						}
					}
					
					// when passing the namespace and we realize that 
					// this is not an article, and that the option of skipping
					// non-article pages is on, we simply skip everything until
					// the closing </page>
					else if (skipped && flag >= 6 && flag < 19) {
						if (b == END_PAGE[i]) {
							i++;
						} else i = 0;
						if (i >= END_PAGE.length) {
							flag = 19;
							return true;
						}
					}

					else if (flag == 6) {
						if (b == START_ID[i]) {
							i++;
						} else i = 0;
						if (i >= START_ID.length) {
							flag = 7;
							return true;
						}
					}

					// put everything in outer <id></id> block into keyBuf
					else if (flag == 7) {
						if (b == END_ID[i]) {
							i++;
						} else i = 0;
						keyBuf.write(b);
						if (i >= END_ID.length) {
							flag = 8;
							return true;
						}
					}

					else if (flag == 8) {
						if (b == START_REVISION[i]) {
							i++;
						} else i = 0;
						if (i >= START_REVISION.length) {
							flag = 9;
							return true;
						}
					}

					// inside <revision></revision> block, first check for id
					else if (flag == 9) {
						if (b == START_ID[i]) {
							i++;
						} else i = 0;
						if (i >= START_ID.length) {
							flag = 10;
							return true;
						}
					}

					// everything inside the inner <id></id> block goes to revision buffer
					else if (flag == 10) {
						if (b == END_ID[i]) {
							i++;
						} else i = 0;
						revIdBuf.write(b);
						if (i >= END_ID.length) {
							flag = 11;
							return true;
						}
					}

					// after the inner <id>, check for either <timestamp> or <parentId>
					else if (flag == 11) {
						int curMatch = 0;				
						if ((i < START_PARENT_ID.length && b == START_PARENT_ID[i]) 
								&& (i < START_TIMESTAMP.length && b == START_TIMESTAMP[i])) {
							curMatch = 3;
						} else if (i < START_PARENT_ID.length && b == START_PARENT_ID[i]) {
							curMatch = 1;
						} else if (i < START_TIMESTAMP.length && b == START_TIMESTAMP[i]) {
							curMatch = 2;
						}				
						if (curMatch > 0 && (i == 0 || parOrTs == 3 || curMatch == parOrTs)) {					
							i++;			
							parOrTs = curMatch;
						} else i = 0;
						if ((parOrTs == 2 || parOrTs == 3) && i >= START_TIMESTAMP.length) {
							flag = 12;
							parOrTs = -1;
							return true;							
						} else if ((parOrTs == 1 || parOrTs == 3) && i >= START_PARENT_ID.length) {
							flag = 14;
							parOrTs = -1;
							return true;
						}		
					}

					// inside <timestamp></timestamp> block everything goes to timestamp buffer
					else if (flag == 12) {
						if (b == END_TIMESTAMP[i]) {
							i++;
						} else i = 0;
						timestampBuf.write(b);
						if (i >= END_TIMESTAMP.length) {
							flag = 13;
							return true;
						}
					}

					// inside <parentId></parentId> block everything goes to parentId buffer
					else if (flag == 14) {
						if (b == END_PARENT_ID[i]) {
							i++;
						} else i = 0;
						parBuf.write(b);
						if (i >= END_PARENT_ID.length) {
							flag = 15;
							return true;
						}
					}

					// after the </parentId>, search for <timestamp>
					else if (flag == 15) {
						if (b == START_TIMESTAMP[i]) {
							i++;
						} else i = 0;
						if (i >= START_TIMESTAMP.length) {
							flag = 12;
							return true;
						}
					}

					// after the </timestamp>, check for <text>
					else if (flag == 13) {
						if (b == START_TEXT[i]) {
							i++;
						} else i = 0;
						if (i >= START_TEXT.length) {
							flag = 16;
							return true;
						}
					}

					// inside <text></text> block everything goes to content buffer
					else if (flag == 16) {
						if (b == END_TEXT[i]) {
							i++;
						} else i = 0;
						contentBuf.write(b);
						if (i >= END_TEXT.length) {
							flag = 17;
							return true;
						}
					}

					// look for the closing </revision>
					else if (flag == 17) {
						if (b == END_REVISION[i]) {
							i++;
						} else i = 0;
						if (i >= END_REVISION.length) {
							flag = 18;
							return true;
						}
					}

					// Flag 16 can be the signal of a new record inside one old page
					else if (flag == 18) {
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
							flag = 19;
							revOrPage = -1;
							return true;							
						} else if ((revOrPage == 1 || revOrPage == 3) && i >= START_REVISION.length) {
							flag = 9;
							revOrPage = -1;
							return true;
						}				
					} 
				}		
			}
		}
	}
}