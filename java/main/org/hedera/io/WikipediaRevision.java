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
 * @author tuan
 *
 */
public class WikipediaRevision extends WikipediaRevisionHeader implements Writable {

	private byte[] text;
		
	public byte[] getText() {
		return text;
	}
	
	public void loadText(byte[] buffer, int offset, int len) {
		setLength(len);
		text = new byte[len];
		System.arraycopy(buffer, offset, text, 0, len);
	}

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
