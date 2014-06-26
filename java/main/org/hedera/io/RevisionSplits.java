package org.hedera.io;

import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/** represents a list of file splits for one single revision dump file */
public class RevisionSplits implements Writable {

	// path to the file in HDFS
	private String filePath;
	private Path path;
	
	// 3 synchronized arrays for offsets, length and hosts of the splits
	private LongArrayList starts;
	private LongArrayList lengths;
	private List<String[]> hosts;
	
	public RevisionSplits() {
		starts = new LongArrayList();
		lengths = new LongArrayList();
		hosts = new ArrayList<>();
	}
	
	public FileSplit split(int i) {
		if (i >= starts.size()) {
			throw new IndexOutOfBoundsException("index goes beyond" +
					" split range: " + i);
		}
		if (path == null) {
			path = new Path(filePath);
		}
		return new FileSplit(path, starts.get(i), lengths.get(i),
				hosts.get(i));
	}
	
	public FileSplit[] splits() {
		int n = starts.size();
		FileSplit[] splits = new FileSplit[n];
		for (int i = 0; i < n; i++) {
			splits[i] = split(i);
		}
		return splits;
	}
	
	public void clear() {
		this.filePath = null;
		this.path = null;
		this.starts.clear();
		this.lengths.clear();
		this.hosts.clear();
	}
	
	public String getFilePath() {
		return filePath;
	}
	
	public void setFilePath(String p) {
		filePath = p;
	}
	
	public void add(long start, long length, String[] host) {
		starts.add(start);
		lengths.add(length);
		hosts.add(host);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		filePath = in.readUTF();
		int size = in.readInt();
		starts = new LongArrayList(size);
		lengths = new LongArrayList(size);
		hosts = new ArrayList<>(size);
		for (int i = 0; i < size; i++) starts.add(in.readLong());
		for (int i = 0; i < size; i++) lengths.add(in.readLong());
		for (int i = 0; i < size; i++) {
			int len = in.readInt();
			String[] h = new String[len];
			for (int j = 0; j < len; j++) {
				h[j] = in.readUTF();
			}
			hosts.add(h);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(filePath);
		int n = starts.size();
		out.writeInt(n);
		for (long l : starts) out.writeLong(l);
		for (long l : lengths) out.writeLong(l);
		for (String[] h : hosts) {
			out.write(h.length);
			for (String s : h) out.writeUTF(s);
		}
	}	
}
