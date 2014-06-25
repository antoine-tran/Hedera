package org.hedera.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/** A splitUnit repsents a set of blocks in a file in HDFS
 * that the mapper can read in parallel */
public class SplitUnit implements WritableComparable<SplitUnit> {

	// path to the physical file in HDFS
	private String filePath;
	
	// start and end pointers of the block
	private long start, length;
	
	// list of hosts
	private String[] hosts;
	
	public SplitUnit(String filePath, long start, long length, String[] hosts) {
		this.filePath = filePath;
		this.start = start;
		this.length = length;
		this.hosts = hosts;
	}

	public SplitUnit() {
	}

	/**
	 * @return the filePath
	 */
	public String getFilePath() {
		return filePath;
	}

	/**
	 * @param filePath the filePath to set
	 */
	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}

	/**
	 * @return the start
	 */
	public long getStart() {
		return start;
	}

	/**
	 * @param start the start to set
	 */
	public void setStart(long start) {
		this.start = start;
	}

	/**
	 * @return the length
	 */
	public long getLength() {
		return length;
	}

	/**
	 * @param length the length to set
	 */
	public void setLength(long length) {
		this.length = length;
	}

	/**
	 * @return the hosts
	 */
	public String[] getHosts() {
		return hosts;
	}

	/**
	 * @param hosts the hosts to set
	 */
	public void setHosts(String[] hosts) {
		this.hosts = hosts;
	}
	
	/**
	 * Get the FileSplit from this unit
	 */
	public FileSplit fileSplit() {
		return new FileSplit(new Path(filePath), start, length, hosts);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		String path = in.readUTF();
		start = in.readLong();
		length = in.readLong();
		int len = in.readInt();
		hosts = new String[len];
		for (int i = 0; i < len; i++) {
			hosts[i] = in.readUTF();
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(filePath);
		out.writeLong(start);
		out.writeLong(length);
		out.writeInt(hosts.length);
		for (String h : hosts) {
			out.writeUTF(h);
		}
	}

	@Override
	public int compareTo(SplitUnit o) {
		return Long.compare(start, o.start);
	}

}
