/**
 * 
 */
package org.hedera.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.Writable;

/**
 * This represents the concatenated bag of words for a set of Wikipedia revisions
 * belonging to the same page within a specific time interval. Every deleted, modifed,
 * inserted words are concatenated
 * @author tuan
 *
 */
public class WikipediaRevisionBOW extends WikipediaRevision implements Writable {
	
	/** list of differentials */
	private List<String> patches;
	
	/** Cached string of the last patched revision in memory. Not sent through
	 * Hadoop spills */
	private String lastRevision;
	
	// last timestamps of the patched revision
	private long lastTimestamp;
	
	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);		
		int patchLen = in.readInt();
		patches = new LinkedList<>();
		for (int i = 0; i < patchLen; i++) {
			patches.add(in.readUTF());
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);		
		int patchSize = patches.size();
		out.writeInt(patchSize);
		for (String p : patches) {
			out.writeUTF(p);
		}
	}
	
	/** Get the String representation of this BOW. 
	 * This is an expensive method, it outputs a chunk of text
	 * @return the original text concatenated with its patches
	 */
	@Override
	public String toString() {
		// heuristics to optimize the memory consumption: Each patch word
		// has in average 5 characters
		StringBuilder sb = new StringBuilder(getLength() + patches.size() * 10);
		String origText = new String(getText(), StandardCharsets.UTF_8);
		sb.append(origText);
		for (String s : patches) {
			sb.append(" ");			
			sb.append(s);
		}
		return sb.toString();
	}
	
	public void addPatch(String word) {
		if (patches == null) {
			patches = new LinkedList<>();			
		}
		patches.add(word);
	}
	
	public void clear() {
		super.clear();
		patches = null;
	}
}
