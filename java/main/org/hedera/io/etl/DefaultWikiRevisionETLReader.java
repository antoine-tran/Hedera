package org.hedera.io.etl;

import static org.hedera.io.input.WikiRevisionInputFormat.SKIP_NON_ARTICLES;
import static org.hedera.io.input.WikiRevisionInputFormat.START_PAGE;
import static org.hedera.io.input.WikiRevisionInputFormat.START_TITLE;
import static org.hedera.io.input.WikiRevisionInputFormat.END_TITLE;
import static org.hedera.io.input.WikiRevisionInputFormat.START_NAMESPACE;
import static org.hedera.io.input.WikiRevisionInputFormat.END_NAMESPACE;
import static org.hedera.io.input.WikiRevisionInputFormat.START_ID;
import static org.hedera.io.input.WikiRevisionInputFormat.END_ID;
import static org.hedera.io.input.WikiRevisionInputFormat.START_REVISION;

import java.io.IOException;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.hedera.WikipediaRevisionHeader;
import org.mortbay.log.Log;


public abstract class DefaultWikiRevisionETLReader<KEYIN, VALUEIN> extends
WikiRevisionETLReader<KEYIN, VALUEIN, WikipediaRevisionHeader> {

	// option to whether skip non-article pages
	protected boolean skipNonArticles = false;

	@Override
	public void initialize(InputSplit input, TaskAttemptContext tac)
			throws IOException, InterruptedException {
		super.initialize(input, tac);
		skipNonArticles = tac.getConfiguration()
				.getBoolean(SKIP_NON_ARTICLES, false);
	}

	@Override
	protected WikipediaRevisionHeader initializeMeta() {		
		return new WikipediaRevisionHeader();		
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
	// 9 - just passed the (next) <revision>
	protected Ack readToPageHeader(WikipediaRevisionHeader meta) 
			throws IOException {
		int i = 0;
		int flag = 1;		
		boolean skipped = false;
		try (DataOutputBuffer pageTitle = new DataOutputBuffer(); 
				DataOutputBuffer nsBuf = new DataOutputBuffer(); 
				DataOutputBuffer keyBuf = new DataOutputBuffer()) {

			while (true) {
				if (!fetchMore()) return Ack.EOF;
				while (hasData()) {
					byte b = nextByte();				
					if (flag == 1) {
						if (b == START_PAGE[i]) {
							i++;
							if (i >= START_PAGE.length) {
								flag = 2;
							}
						} else i = 0;
					}

					else if (flag == 2) {
						if (b == START_TITLE[i]) {
							i++;
						} else i = 0;
						if (i >= START_TITLE.length) {
							flag = 3;
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
						}
					}

					else if (flag == 4) {
						if (b == START_NAMESPACE[i]) {
							i++;
						} else i = 0;
						if (i >= START_NAMESPACE.length) {
							flag = 5;
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
						}
					}

					// when passing the namespace and we realize that 
					// this is not an article, and that the option of skipping
					// non-article pages is on, we simply skip everything until
					// the closing </page>				
					else if (skipped && flag >= 6) {
						Log.warn("Peculiar read after skipping namespace");
						/* if (b == END_PAGE[i]) {
						i++;
						} else i = 0;
						if (i >= END_PAGE.length) {
							return Ack.SKIPPED;
						} */
						return Ack.FAILED;
					}

					else if (flag == 6) {
						if (b == START_ID[i]) {
							i++;
						} else i = 0;
						if (i >= START_ID.length) {
							flag = 7;
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
						}
					}

					else if (flag == 8) {
						if (b == START_REVISION[i]) {
							i++;
						} else i = 0;
						if (i >= START_REVISION.length) {
							flag = 9;
							return Ack.PASSED_TO_NEXT_TAG;
						}
					}
				}
			}
		}
	}
}

