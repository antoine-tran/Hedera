package org.hedera;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.hedera.io.WikipediaLinkSnapshot;
import org.hedera.io.WikipediaLinkSnapshot.Link;
import org.junit.Test;

public class TestWikiETLReaders {

	// @Test
	public void testLinkReader() {
		LocalWikiRevisionLinkReader reader = new LocalWikiRevisionLinkReader();
		
		try {
			reader.initialize();
			while (reader.nextKeyValue()) {
				LongWritable key = reader.getCurrentKey();
				WikipediaLinkSnapshot wls = reader.getCurrentValue();
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
	
	@Test
	public void testArticleSkipping() {
		LocalWikiRevisionLinkReader reader = new LocalWikiRevisionLinkReader();
		reader.skipNonArticles = true;
		
		try {
			reader.initialize();
			while (reader.nextKeyValue()) {
				LongWritable key = reader.getCurrentKey();
				WikipediaLinkSnapshot wls = reader.getCurrentValue();
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
}