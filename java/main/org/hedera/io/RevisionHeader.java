package org.hedera.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/** a wikipedia header that provides APIs to access revision meta-data */
public class RevisionHeader implements Writable, 
		CloneableObject<RevisionHeader> {

	private long pageId;
	private long revisionId;
	private long parentId;
	private long timestamp;	
	private String pageTitle;
	private int namespace;
	private int length;
	private boolean minor = false;
		
	public boolean isMinor() {
		return minor;
	}
	public void setMinor(boolean minor) {
		this.minor = minor;
	}
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
	public int getLength() {
		return length;
	}
	public void setLength(int length) {
		this.length = length;
	}
	
	public void clear() {
		this.pageId = this.revisionId = this.parentId 
				= this.timestamp = this.length = 0;
		this.namespace = 0;
		this.pageTitle = null;
	}
	
	@Override
	public void clone(RevisionHeader obj) {
		this.pageId = obj.pageId;
		this.namespace = obj.namespace;
		this.length = obj.length;
		this.pageTitle = obj.pageTitle;
		this.parentId = obj.parentId;
		this.revisionId = obj.revisionId;
		this.timestamp = obj.timestamp;
	}
	
	@Override
	public String toString() {
		return "[page: " + pageId + ", rev: " + revisionId + ", par: "
				+ parentId + ", timestamp: " + timestamp + ", namespace: "
				+ namespace + ", length: " + length + ", title: "
				+ (pageTitle == null ? "null" : pageTitle) + "]";
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		pageId = in.readLong();
		revisionId = in.readLong();
		parentId = in.readLong();
		timestamp = in.readLong();
		namespace = in.readInt();
		length = in.readInt();
		pageTitle = in.readUTF();
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(pageId);
		out.writeLong(revisionId);
		out.writeLong(parentId);
		out.writeLong(timestamp);
		out.writeInt(namespace);
		out.writeInt(length);
		out.writeUTF(pageTitle);		
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == this) return true;
		if (!(obj instanceof RevisionHeader)) return false;
		RevisionHeader wrh = (RevisionHeader)obj;
		return (wrh.pageId == pageId && wrh.revisionId == revisionId
				&& wrh.namespace == namespace);
	}
	
	@Override
	public int hashCode() {
		return (int) (pageId + revisionId + namespace);
	}
}
