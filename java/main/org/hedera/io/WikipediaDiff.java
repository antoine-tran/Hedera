/**
 * 
 */
package org.hedera.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import name.fraser.neil.plaintext.diff_match_patch.Diff;
import name.fraser.neil.plaintext.diff_match_patch.Operation;

import org.apache.hadoop.io.Writable;
import org.python.indexer.ast.NYield;

/**
 * @author tuan
 *
 */
public class WikipediaDiff implements Writable {

	private long pageId;
	private long revisionId;
	private long parentId;
	private long timestamp;
	private String pageTitle;
	private LinkedList<Diff> diffs;
	
	/**
	 * @return the pageId
	 */
	public long getPageId() {
		return pageId;
	}

	/**
	 * @param pageId the pageId to set
	 */
	public void setPageId(long pageId) {
		this.pageId = pageId;
	}

	/**
	 * @return the revisionId
	 */
	public long getRevisionId() {
		return revisionId;
	}

	/**
	 * @param revisionId the revisionId to set
	 */
	public void setRevisionId(long revisionId) {
		this.revisionId = revisionId;
	}

	/**
	 * @return the parentId
	 */
	public long getParentId() {
		return parentId;
	}

	/**
	 * @param parentId the parentId to set
	 */
	public void setParentId(long parentId) {
		this.parentId = parentId;
	}

	/**
	 * @return the timestamp
	 */
	public long getTimestamp() {
		return timestamp;
	}

	/**
	 * @param timestamp the timestamp to set
	 */
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	/**
	 * @return the pageTitle
	 */
	public String getPageTitle() {
		return pageTitle;
	}

	/**
	 * @param pageTitle the pageTitle to set
	 */
	public void setPageTitle(String pageTitle) {
		this.pageTitle = pageTitle;
	}

	/**
	 * @return the diffs
	 */
	public LinkedList<Diff> getDiffs() {
		return diffs;
	}
	
	public void setDiffs(LinkedList<Diff> diffToSet) {
		diffs = diffToSet;
	}
	
	private Operation byte2opt(byte sig) {
		if (sig == -1) {
			return Operation.DELETE;
		} else if (sig == 1) {
			return Operation.INSERT;
		} else return Operation.EQUAL;
	}
	
	private byte opt2byte(Operation opt) {
		if (opt == Operation.DELETE) {
			return -1;
		} else if (opt == Operation.INSERT) {
			return 1;
		} else return 0;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		pageId = in.readLong();
		revisionId = in.readLong();
		parentId = in.readLong();
		timestamp = in.readLong();
		int length = in.readInt();
		this.diffs = new LinkedList<>();
		for (int i = 0; i < length; i++) {
			
			// using byte to optimize the memory sent to the cluster
			Operation opt = byte2opt(in.readByte());
			byte len = in.readByte();
			byte[] word = new byte[len];
			in.readFully(word, 0, len);
			
			Diff d = new Diff(opt, new String(word,"UTF-8"));
			this.diffs.add(d);
		}
		this.pageTitle = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(pageId);
		out.writeLong(revisionId);
		out.writeLong(parentId);
		out.writeLong(timestamp);
		out.writeInt(diffs.size());
		for (int i = 0; i < diffs.size(); i++) {
			Diff d = diffs.get(i);
			out.writeByte(opt2byte(d.operation));
			byte[] bytes = d.text.getBytes("UTF-8");
			out.writeByte(bytes.length);
			out.write(bytes,0,bytes.length);
		}
		out.writeUTF(pageTitle);
	}
}
