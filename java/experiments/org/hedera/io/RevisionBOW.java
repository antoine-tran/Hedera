/**
 * 
 */
package org.hedera.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.io.Writable;

import edu.umd.cloud9.io.map.HMapSIW;
import edu.umd.cloud9.util.map.MapKI.Entry;

/**
 * A revision that 
 * @author tuan
 *
 */
public class RevisionBOW implements Writable {

	private long pageId;
	private long revisionId;
	private long timestamp;
	private int namespace;
	
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
	
	// last revision which the diff algorithm is applied against of
	private long lastTimestamp;
	
	// revision id of which the diff algo is applied against.
	// we chose not to use parentId to avoid conflict of semantics
	private long lastRevisionId;
	
	// the bag-of-words stored in a map
	private HMapSIW bow;
	
	public Iterator<Entry<String>> getWords() {
		return bow.entrySet().iterator();
	}
	
	/**
	 * @return the lastTimestamp
	 */
	public long getLastTimestamp() {
		return lastTimestamp;
	}

	/**
	 * @param lastTimestamp the lastTimestamp to set
	 */
	public void setLastTimestamp(long lastTimestamp) {
		this.lastTimestamp = lastTimestamp;
	}

	/**
	 * @return the lastRevisionId
	 */
	public long getLastRevisionId() {
		return lastRevisionId;
	}

	/**
	 * @param lastRevisionId the lastRevisionId to set
	 */
	public void setLastRevisionId(long lastRevisionId) {
		this.lastRevisionId = lastRevisionId;
	}
	
	/** build the bag-of-word model here.
	 * TODO: Apply some Stemmer here */
	public void buildBOW(List<String> words) {
		if (bow == null) {
			bow = new HMapSIW();
		}
		for (String w : words) {
			int cnt = 0;
			if (bow.containsKey(w)) {
				cnt = bow.get(w);
			}
			bow.put(w, cnt + 1);
		}
	}
	
	/** update the bag of words with new word */
	public void updateBOW(String w) {
		if (bow == null) {
			bow = new HMapSIW();
		}
		int cnt = 0;
		if (bow.containsKey(w)) {
			cnt = bow.get(w);
		}
		bow.put(w, cnt + 1);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// First read the header
		pageId = in.readLong();
		revisionId = in.readLong();
		timestamp = in.readLong();
		namespace = in.readInt();
		
		// Second read the last revision's id and timestamp
		lastRevisionId = in.readLong();
		lastTimestamp = in.readLong();
		
		// Finally read the Bag of words
		bow = HMapSIW.create(in);
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// writing order: revision header, last revision id and timestamp,
		// map of BoW
		out.writeLong(pageId);
		out.writeLong(revisionId);
		out.writeLong(timestamp);
		out.writeInt(namespace);
		out.writeLong(lastRevisionId);
		out.writeLong(lastTimestamp);
		bow.write(out);
	}
	
	public void clear() {
		this.pageId = this.revisionId = this.timestamp =  0;
		this.namespace = 0;
		this.lastTimestamp = -1;
		this.lastRevisionId = -1;
		if (bow != null) {
			bow.clear();
		}
	}
}
