package org.hedera.io.etl;

import static org.hedera.io.input.WikiRevisionInputFormat.SKIP_NON_ARTICLES;
import static org.hedera.io.input.WikiRevisionInputFormat.SKIP_REDIRECT;
import static org.hedera.io.input.WikiRevisionInputFormat.START_TITLE;
import static org.hedera.io.input.WikiRevisionInputFormat.END_TITLE;
import static org.hedera.io.input.WikiRevisionInputFormat.START_NAMESPACE;
import static org.hedera.io.input.WikiRevisionInputFormat.END_NAMESPACE;
import static org.hedera.io.input.WikiRevisionInputFormat.START_ID;
import static org.hedera.io.input.WikiRevisionInputFormat.END_ID;
import static org.hedera.io.input.WikiRevisionInputFormat.START_REVISION;
import static org.hedera.io.input.WikiRevisionInputFormat.START_REDIRECT;

import java.io.IOException;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.hedera.io.RevisionHeader;
import org.mortbay.log.Log;

/**
 * A default WikiRevisionETLReader that extracts title, page id, namespace
 * from the page header
 */
public abstract class DefaultRevisionETLReader<KEYIN, VALUEIN> extends
		RevisionETLReader<KEYIN, VALUEIN, RevisionHeader> {

	private static final Logger LOG = 
			Logger.getLogger(DefaultRevisionETLReader.class);

	// option to whether skip non-article or redirect pages
	protected boolean skipNonArticles = false;
	protected boolean skipRedirect = false;

	@Override
	public void initialize(InputSplit input, TaskAttemptContext tac)
			throws IOException, InterruptedException {
		super.initialize(input, tac);
		skipNonArticles = tac.getConfiguration()
				.getBoolean(SKIP_NON_ARTICLES, false);
		skipRedirect = tac.getConfiguration()
				.getBoolean(SKIP_REDIRECT, false);

		LOG.info("Splitting option: [skip non-article: "
				+ skipNonArticles + ", skip redirect: "
				+ SKIP_REDIRECT + "]");
	}

	@Override
	protected RevisionHeader initializeMeta() {		
		return new RevisionHeader();		
	}

	@Override
	// Read the page header 
	// -1: EOF
	// 1 - outside the <page> tag
	// 2 - just passed the <page> tag but outside the <title>
	// 3 - just passed the <title> tag		
	// 4 - just passed the </title> tag but outside the <namespace>
	// 5 - just passed the <namespace>
	// 6 - just passed the </namespace> but outside the <id>
	// 7 - just passed the (page's) <id>
	// 8 - just passed the </id> tag but outside the <revision>	
	// 9 - (optionally) just passed the <redirect>
	// 10 - just passed the (next) <revision>
	protected Ack readToPageHeader(RevisionHeader meta) 
			throws IOException {
		int i = 0;
		int flag = 2;		
		boolean skipped = false;
		int revOrRedirect = -1;
		try (DataOutputBuffer pageTitle = new DataOutputBuffer(); 
				DataOutputBuffer nsBuf = new DataOutputBuffer(); 
				DataOutputBuffer keyBuf = new DataOutputBuffer()) {

			while (true) {
				if (!fetchMore()) return Ack.EOF;
				while (hasData()) {
					byte b = nextByte();	

					// when passing the namespace and we realize that 
					// this is not an article, and that the option of skipping
					// non-article pages is on, we simply skip everything until
					// the closing </page>				
					if (skipped) {
						if (flag >= 6) {
							Log.warn("Peculiar read after skipping namespace");
							/* 
							if (b == END_PAGE[i]) {
								i++;
							} else i = 0;
							if (i >= END_PAGE.length) {
								return Ack.SKIPPED;
							} */
							return Ack.FAILED;
						} else return Ack.SKIPPED;
					}

					if (flag == 2) {
						if (b == START_TITLE[i]) {
							i++;
						} else i = 0;
						if (i >= START_TITLE.length) {
							flag = 3;
							i = 0;
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
							String title = new String(pageTitle.getData(), 0, 
									pageTitle.getLength() - END_TITLE.length);
							meta.setPageTitle(title);
							pageTitle.reset();
							i = 0;
						}
					}

					else if (flag == 4) {
						if (b == START_NAMESPACE[i]) {
							i++;
						} else i = 0;
						if (i >= START_NAMESPACE.length) {
							flag = 5;
							i = 0;
						}
					}

					else if (flag == 5) {
						if (b == END_NAMESPACE[i]) {
							i++;
						} else i = 0;
						nsBuf.write(b);
						if (i >= END_NAMESPACE.length) {
							flag = 6;
							String nsStr = new String(nsBuf.getData(), 0, 
									nsBuf.getLength() - END_NAMESPACE.length);
							int ns = Integer.parseInt(nsStr);
							nsBuf.reset();
							if (ns != 0) {
								if (skipNonArticles) {
									skipped = true;
									meta.clear();
									return Ack.SKIPPED;
								}
							}
							meta.setNamespace(ns);
							i = 0;
						}
					}

					else if (flag == 6) {
						if (b == START_ID[i]) {
							i++;
						} else i = 0;
						if (i >= START_ID.length) {
							flag = 7;
							i = 0;
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
							String idStr = new String(keyBuf.getData(), 0,
									keyBuf.getLength() - END_ID.length);
							long pageId = Long.parseLong(idStr);
							meta.setPageId(pageId);
							i = 0;
						}
					}

					else if (flag == 8) {
						int curMatch = 0;						
						if ((i < START_REVISION.length 
								&& b == START_REVISION[i]) 
								&& (i < START_REDIRECT.length 
								&& b == START_REDIRECT[i])
								
								// subtle bug here: some tag names can overlap
								// multiple times
								&& (revOrRedirect == 3 
								|| revOrRedirect == -1)){
							curMatch = 3;
						} else if (i < START_REVISION.length 
								&& b == START_REVISION[i]
								&& revOrRedirect != 2) {
							curMatch = 1;
						} else if (i < START_REDIRECT.length 
								&& b == START_REDIRECT[i]
								&& revOrRedirect != 1) {
							curMatch = 2;
						} else {
							curMatch = 0;
						}
						if (curMatch > 0 && (i == 0 || revOrRedirect == 3 
								|| curMatch == revOrRedirect)) {					
							i++;			
							revOrRedirect = curMatch;
						} else i = 0;
						if ((revOrRedirect == 2 || revOrRedirect == 3) 
								&& i >= START_REDIRECT.length) {
							if (skipRedirect) {
								skipped = true;
								meta.clear();
								return Ack.SKIPPED;
							}
							revOrRedirect = -1;	
							flag = 9;
							i = 0;
						} else if ((revOrRedirect == 1 || revOrRedirect == 3) 
								&& i >= START_REVISION.length) {
							flag = 10;
							revOrRedirect = -1;
							return Ack.PASSED_TO_NEXT_TAG;
						}							
					}
					
					else if (flag == 9 && !skipRedirect) {
						if (b == START_REVISION[i]) {
							i++;
						} else i = 0;
						if (i >= START_REVISION.length) {
							flag = 10;
							return Ack.PASSED_TO_NEXT_TAG;
						}
					}					
				}
			}
		}
	}
}

