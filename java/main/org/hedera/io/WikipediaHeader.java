package org.hedera.io;

/**
 * a header that contains basic APIs for a wikipedia page or revision
 * @author tuan
 *
 */
public interface WikipediaHeader {

	/** access to page id */
	public long getPageId();
	public void setPageId(long pageId);
	
	/** access to page title */
	public String getPageTitle();
	public void setPageTitle(String title);
}
