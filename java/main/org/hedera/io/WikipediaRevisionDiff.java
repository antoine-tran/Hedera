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

import difflib.ChangeDelta;
import difflib.Chunk;
import difflib.DeleteDelta;
import difflib.Delta;
import difflib.Delta.TYPE;
import difflib.InsertDelta;

/**
 * A writable object that represents diff between two Wikipedia revisions
 * @author tuan
 *
 */
public class WikipediaRevisionDiff implements Writable {

	private long pageId;
	private long revisionId;
	private long parentId;
	private long timestamp;
	private String pageTitle;
	private LinkedList<Delta> diffs;
	
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
	public LinkedList<Delta> getDiffs() {
		return diffs;
	}
	
	public void setDiffs(LinkedList<Delta> diffToSet) {
		diffs = diffToSet;
	}
	
	public void add(Delta d) {
		if (diffs == null) {
			diffs = new LinkedList<>();			
		}
		diffs.add(d);
	}
	
	public void clear() {
		this.diffs.clear();
		this.pageTitle = null;
		this.parentId = -1;
		this.pageId = -1;
		this.revisionId = -1;
		this.timestamp = 0;
	}
	
	private TYPE byte2opt(byte sig) {
		if (sig == 0) {
			return TYPE.DELETE;
		} else if (sig == 1) {
			return TYPE.INSERT;
		} else if (sig == 2) {
			return TYPE.CHANGE;
		} else throw new IllegalArgumentException(
				"Uknown TYPE signature: " + sig);
	}
	
	private byte opt2byte(TYPE opt) {
		if (opt == TYPE.DELETE) {
			return 0;
		} else if (opt == TYPE.INSERT) {
			return 1;
		} else return 2;
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
			Delta d = readDelta(in);
			diffs.add(d);
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
			Delta d = diffs.get(i);
			writeDelta(out, d);
		}
		out.writeUTF(pageTitle);
	}
	
	// reconstruct the delta from the input stream
	private Delta readDelta(DataInput in) throws IOException {
		
		// read meta-data
		TYPE opt = byte2opt(in.readByte());
		
		int origPos = in.readInt();
		int reviPos = in.readInt();		
		int originalLen = in.readInt();
		int revisedLen = in.readInt();
		  
		// first read the original
		List<String> original = new LinkedList<>();
		for (int i = 0; i < originalLen; i++) {
			int len = in.readInt();
			byte[] rawText = new byte[len];
			in.readFully(rawText, 0, len);
			original.add(new String(rawText, "UTF-8"));
		}		
		Chunk orig = new Chunk(origPos, original);		
		
		// second read the revised
		List<String> revised = new LinkedList<>();
		for (int i = 0; i < revisedLen; i++) {
			int len = in.readInt();
			byte[] rawText = new byte[len];
			in.readFully(rawText, 0, len);
			revised.add(new String(rawText, "UTF-8"));
		}
		Chunk revi = new Chunk(reviPos, revised);
		
		if (opt == TYPE.CHANGE) {
			return new ChangeDelta(orig, revi);
		}
		else if (opt == TYPE.DELETE) {
			return new DeleteDelta(orig, revi);
		}
		else if (opt == TYPE.INSERT) {
			return new InsertDelta(orig, revi);
		}
		
		else throw new IllegalArgumentException("Unknown type: " + opt);
 	}
	
	private void writeDelta(DataOutput out, Delta d) throws IOException {

		// write meta-data
		out.writeByte(opt2byte(d.getType()));
				
		Chunk orig = d.getOriginal();
		Chunk revi = d.getRevised();
		
		out.writeInt(orig.getPosition());
		out.writeInt(revi.getPosition());
		
		int originalLen = orig.getLines().size(); 
		out.writeInt(originalLen);
		
		int revisedLen = revi.getLines().size();
		out.writeInt(revisedLen);
		  
		// first write the original
		for (Object o : orig.getLines()) {
			byte[] rawText = ((String)o).getBytes("UTF-8");
			out.writeInt(rawText.length);
			out.write(rawText, 0, rawText.length);			
		}				
		
		// second write the revised
		for (Object o : revi.getLines()) {
			byte[] rawText = ((String)o).getBytes("UTF-8");
			out.writeInt(rawText.length);
			out.write(rawText, 0, rawText.length);			
		}
	}
}
