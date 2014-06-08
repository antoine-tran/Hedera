/**
 * 
 */
package org.hedera.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * This represents the concatenated bag of words for a set of Wikipedia revisions
 * belonging to the same page within a specific time interval. Every deleted, modifed,
 * inserted words are concatenated
 * @author tuan
 *
 */
public class WikipediaRevisionBOW extends WikipediaRevisionHeader implements Writable {

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
	}

}
