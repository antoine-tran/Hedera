/**
 * 
 */
package org.hedera.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * A writable object consists of two long and one text values.
 * Comparision is based on the two long's only
 * @author tuan
 *
 */
public class LongLongStringWritable implements WritableComparable<LongLongStringWritable> {

	private long first;
	private long second;
	private String text;
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public int compareTo(LongLongStringWritable o) {
		// TODO Auto-generated method stub
		return 0;
	}

}
