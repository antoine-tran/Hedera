/**
 * 
 */
package org.hedera.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.Writable;

/**
 * This object represents the outlink profile of a Wikipedia page at a specific moment
 * @author tuan
 *
 */
public class WikipediaLinkSnapshot implements Writable {

	private long pageId;
	private long revisionId;
	private long parentId;
	private long timestamp;	
	private String pageTitle;
	private int namespace;
	private List<Link> links; 

	public static class Link {
		private String anchor;
		private String target;

		public Link(String anchor, String target) {
			this.anchor = anchor;
			this.target = target;
		}

		public String getAnchorText() {
			return anchor;
		}

		public String getTarget() {
			return target;
		}

		public String toString() {
			return String.format("[target: %s, anchor: %s]", target, anchor);
		}
	}
	
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

	public int getNamespace() {
		return namespace;
	}

	public void setNamespace(int namespace) {
		this.namespace = namespace;
	}
	
	public List<Link> getLinks() {
		return links;
	}
	
	public void addLink(Link l) {
		if (links == null) {
			links = new LinkedList<>();
		}
		links.add(l);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		pageId = in.readLong();
		revisionId = in.readLong();
		parentId = in.readLong();
		timestamp = in.readLong();
		namespace = in.readInt();
		int len = in.readInt();
		for (int i = 0; i < len; i++) {
			int anchorLen = in.readInt();
			byte[] anchorBytes = new byte[anchorLen];
			in.readFully(anchorBytes, 0, anchorLen);
			String anchor = new String(anchorBytes, "UTF-8");
			
			int textLen = in.readInt();
			byte[] textBytes = new byte[textLen];
			in.readFully(textBytes, 0, textLen);
			String text = new String(textBytes, "UTF-8");
			
			Link l = new Link(anchor, text);
			addLink(l);
		}
		pageTitle = in.readUTF();
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(pageId);
		out.writeLong(revisionId);
		out.writeLong(parentId);
		out.writeLong(timestamp);
		out.writeInt(namespace);
		if (links == null)
			out.writeInt(0);
		else
			out.writeInt(links.size());
		for (Link l : links) {
			byte[] bytes = l.anchor.getBytes("UTF-8");
			out.writeInt(bytes.length);
			out.write(bytes, 0, bytes.length);
			bytes = l.target.getBytes("UTF-8");
			out.writeInt(bytes.length);
			out.write(bytes, 0, bytes.length);
		}
		out.writeUTF(pageTitle);
	}
}
