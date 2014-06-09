package org.hedera;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.hedera.io.LinkProfile;
import org.hedera.io.LinkProfile.Link;
import org.hedera.io.RevisionBOW;
import org.junit.Test;

import edu.umd.cloud9.util.map.MapKI.Entry;

public class TestWikiETLReaders {

	// @Test
	public void testLinkReader() {
		LocalWikiRevisionLinkReader reader = new LocalWikiRevisionLinkReader();

		try {
			reader.initialize();
			while (reader.nextKeyValue()) {
				LongWritable key = reader.getCurrentKey();
				LinkProfile wls = reader.getCurrentValue();
				System.out.println(key.get());			
				if (wls.getLinks() != null) {
					for (Link l : wls.getLinks()) {
						System.out.println(wls.getPageTitle() + "==>" 
								+ l.getAnchorText() + "," + l.getTarget());
					}
				}
			}
			System.out.println("Finished.");

		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}		
	}

	//@Test
	public void testArticleSkipping() {
		LocalWikiRevisionLinkReader reader = new LocalWikiRevisionLinkReader();
		reader.skipNonArticles = true;

		try {
			reader.initialize();
			while (reader.nextKeyValue()) {
				LongWritable key = reader.getCurrentKey();
				LinkProfile wls = reader.getCurrentValue();
				System.out.println(key.get());			
				if (wls.getLinks() != null) {
					for (Link l : wls.getLinks()) {
						System.out.println(wls.getPageTitle() + "==>" 
								+ l.getAnchorText() + "," + l.getTarget());
					}
				}
			}
			System.out.println("Finished.");

		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}		
	}

	// @Test
	public void testRedirectSkipping() {
		LocalWikiRevisionLinkReader reader = new LocalWikiRevisionLinkReader();
		reader.skipRedirect = true;
		reader.skipNonArticles = false;

		try {
			reader.initialize();
			while (reader.nextKeyValue()) {
				LongWritable key = reader.getCurrentKey();
				LinkProfile wls = reader.getCurrentValue();
				System.out.println(key.get() + "\t" + wls.getRevisionId());			
				if (wls.getLinks() != null) {
					for (Link l : wls.getLinks()) {
						System.out.println(wls.getPageTitle() + "==>" 
								+ l.getAnchorText() + "," + l.getTarget());
					}
				}
			}
			System.out.println("Finished.");

		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}		
	}

	@Test
	public void testBOWReader() {
		LocalWikiRevisionBOWReader reader = new LocalWikiRevisionBOWReader();
		reader.unitInterval = 1000 * 60 * 60 * 24l;

		try {
			reader.initialize();
			while (reader.nextKeyValue()) {
				LongWritable key = reader.getCurrentKey();
				RevisionBOW bow = reader.getCurrentValue();
				System.out.println(key.get() + "\t" + bow.getRevisionId());			
				Iterator<Entry<String>> iter = bow.getWords();
				while (iter.hasNext()) {
					Entry<String> e = iter.next();
					System.out.println(e.getKey() + ":\t" + e.getValue());
				}
			}
			System.out.println("Finished.");

		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}					
	}
}
