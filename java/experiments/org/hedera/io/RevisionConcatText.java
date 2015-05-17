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

/**
 * This represents the concatenated bag of words for a set of Wikipedia revisions
 * belonging to the same page within a specific time interval. Every deleted, modifed,
 * inserted words are concatenated
 * @author tuan
 *
 */
public class RevisionConcatText extends Revision {
	
	/** list of differentials */
	private List<String> patches;
	
	/** Cached string of the last patched revision in memory. Not sent through
	 * Hadoop spills */
	private List<String> lastRevision;
	
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
	
	public List<String> getLastRevision() {
		return lastRevision;
	}
	
	public void setLastRevision(List<String> lastRevision) {
		this.lastRevision = lastRevision;
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
	
	@Override
	public void loadText(byte[] b, int offset, int len) {
		super.loadText(b, offset, len);
		this.lastRevision = new LinkedList<>();
	}
	
	public void addPatch(String word) {
		if (patches == null) {
			patches = new LinkedList<>();			
		}
		patches.add(word);
	}
	
	@Override
	public void clear() {
		super.clear();
		patches = null;
	}
}
