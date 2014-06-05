package org.hedera;

/** Encode the header of a page */
public class WikipediaRevisionHeader {
	
	private long pageId;
	private long revisionId;
	private long parentId;
	private long timestamp;	
	private String pageTitle;
	private int namespace;
	private long length;
	
	public long getPageId() {
		return pageId;
	}
	public void setPageId(long pageId) {
		this.pageId = pageId;
	}
	public long getRevisionId() {
		return revisionId;
	}
	public void setRevisionId(long revisionId) {
		this.revisionId = revisionId;
	}
	public long getParentId() {
		return parentId;
	}
	public void setParentId(long parentId) {
		this.parentId = parentId;
	}
	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	public String getPageTitle() {
		return pageTitle;
	}
	public void setPageTitle(String pageTitle) {
		this.pageTitle = pageTitle;
	}
	public int getNamespace() {
		return namespace;
	}
	public void setNamespace(int namespace) {
		this.namespace = namespace;
	}
	public long getLength() {
		return length;
	}
	public void setLength(long length) {
		this.length = length;
	}
	
	public void clear() {
		this.pageId = this.revisionId = this.parentId 
				= this.timestamp = this.length = 0;
		this.namespace = 0;
		this.pageTitle = null;
	}
}
