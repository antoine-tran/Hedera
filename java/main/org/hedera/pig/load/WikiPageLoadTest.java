package org.hedera.pig.load;

import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.hedera.io.Revision;
import org.hedera.io.input.WikiRevisionPageInputFormat;

public class WikiPageLoadTest extends LoadFunc implements LoadMetadata {
	
	// a cached object that defines the output schema of a Wikipedia page. Use volatile to fix
	// the infamous double-checked locking issue, and to make access to this object thread-safe
	protected volatile ResourceSchema schema;

	private static final WikiRevisionPageInputFormat INPUT_FORMAT = 
			new WikiRevisionPageInputFormat();
	
	protected RecordReader<LongWritable, Revision> reader;
	
	protected TupleFactory tuples;
	protected BagFactory bags;

	@Override
	public String[] getPartitionKeys(String loc, Job job) throws IOException {
		setLocation(loc, job);
		return null;
	}

	@Override
	public ResourceSchema getSchema(String loc, Job job) throws IOException {
		ResourceSchema s = schema;
		if (s == null) {
			synchronized (this) {
				s = schema;
				if (s == null) {					
					defineSchema();
				}
			}
		}
		return schema;
	}

	@Override
	public ResourceStatistics getStatistics(String arg0, Job arg1)
			throws IOException {
		return null;
	}

	@Override
	public void setPartitionFilter(Expression arg0) throws IOException {
	}

	@Override
	public InputFormat getInputFormat() throws IOException {
		return INPUT_FORMAT;
	}
	
	protected void defineSchema() throws FrontendException {
	}

	@Override
	public Tuple getNext() throws IOException {
		boolean hasNext;
		try {
			hasNext = reader.nextKeyValue();
			if (hasNext) {
				LongWritable k = reader.getCurrentKey();
				Revision v = reader.getCurrentValue();
				return tuples.newTupleNoCopy(Arrays.asList(k.get(),
						new String(v.getText(),"UTF-8")));
			}
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void prepareToRead(RecordReader reader, PigSplit split)
			throws IOException {
		this.reader = reader;
		this.tuples = TupleFactory.getInstance();
		this.bags = BagFactory.getInstance();
	}

	@Override
	public void setLocation(String loc, Job job) throws IOException {
		setInputPaths(job, loc);
	}
}
