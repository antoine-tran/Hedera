/**
 * 
 */
package org.hedera.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
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
public class RevisionDiff extends RevisionHeader {

	private LinkedList<Delta> diffs;

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
		super.clear();
		this.diffs.clear();
	}
	
	public static TYPE byte2opt(byte sig) {
		if (sig == 0) {
			return TYPE.DELETE;
		} else if (sig == 1) {
			return TYPE.INSERT;
		} else if (sig == 2) {
			return TYPE.CHANGE;
		} else throw new IllegalArgumentException(
				"Uknown TYPE signature: " + sig);
	}
	
	public static byte opt2byte(TYPE opt) {
		if (opt == TYPE.DELETE) {
			return 0;
		} else if (opt == TYPE.INSERT) {
			return 1;
		} else return 2;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		int length = in.readInt();
		this.diffs = new LinkedList<>();
		for (int i = 0; i < length; i++) {			
			Delta d = readDelta(in);
			diffs.add(d);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeInt(diffs.size());
		for (int i = 0; i < diffs.size(); i++) {
			Delta d = diffs.get(i);
			writeDelta(out, d);
		}
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
