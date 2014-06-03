/**
 * 
 */
package org.hedera.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Provide a data model for one Wikipedia revision that is exchangable within Hadoop settings
 * 
 * This is the full version of a revision. It comprises of:
 * - Page ID
 * - Revision ID
 * - Parent ID (-1 if none)
 * - Timestamp (as epoch long)
 * - Full text
 *
 * @author tuan
 *
 */
public class WikipediaRevision implements Writable {

	private long pageId;
	private long revisionId;
	private long parentId;
	private long timestamp;	
	private String pageTitle;
	private byte[] text;
	private int namespace;
		
	public String getPageTitle() {
		return pageTitle;
	}

	public void setPageTitle(String pageTitle) {
		this.pageTitle = pageTitle;
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

	public byte[] getText() {
		return text;
	}
	
	public int getNamespace() {
		return namespace;
	}

	public void setNamespace(int namespace) {
		this.namespace = namespace;
	}

	public void loadText(byte[] buffer, int offset, int length) {
		text = new byte[length];
		System.arraycopy(buffer, offset, text, 0, length);
	}

	public void clear() {
		this.pageId = 0;
		this.revisionId = 0;
		this.parentId = 0;
		this.timestamp = 0;
		this.pageTitle = null;
		this.text = null;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		pageId = in.readLong();
		revisionId = in.readLong();
		parentId = in.readLong();
		timestamp = in.readLong();
		int length = in.readInt();		
		text = new byte[length];
		in.readFully(text, 0, length);
		pageTitle = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(pageId);
		out.writeLong(revisionId);
		out.writeLong(parentId);
		out.writeLong(timestamp);
		WritableUtils.writeVInt(out, text.length);		
		out.write(text, 0, text.length);
		out.writeUTF(pageTitle);
	}
}
