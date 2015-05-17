/**
 * 
 */
package org.hedera.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Provide a data model for one Wikipedia revision that is exchangable within Hadoop settings
 *
 * @author tuan
 *
 */
public class Revision extends RevisionHeader {

	private byte[] text;
		
	public byte[] getText() {
		return text;
	}
	
	public void loadText(byte[] buffer, int offset, int len) {
		setLength(len);
		text = new byte[len];
		System.arraycopy(buffer, offset, text, 0, len);
	}

	@Override
	public void clear() {
		super.clear();
		this.text = null;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);	
		text = new byte[getLength()];
		in.readFully(text, 0, getLength());
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);	
		out.write(text, 0, getLength());		
	}
}
