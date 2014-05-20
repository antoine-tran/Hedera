package org.wikimedia.pig.load;

import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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

import org.wikimedia.hadoop.io.WikipediaRevisionInputFormat;

public class WikipediaRevisionPairLoaderTest extends LoadFunc implements LoadMetadata {

	private static final WikipediaRevisionInputFormat INPUT_FORMAT = new WikipediaRevisionInputFormat();

	protected RecordReader<LongWritable, Text> reader;
	
	// a cached object that defines the output schema of a Wikipedia page. Use volatile to fix
	// the infamous double-checked locking issue, and to make access to this object thread-safe
	protected volatile ResourceSchema schema;
	
	// protected diff_match_patch dmp;
	
	protected TupleFactory tuples;
	protected BagFactory bags;
	
	@Override
	public InputFormat getInputFormat() throws IOException {
		return INPUT_FORMAT;
	}

	@Override
	public void prepareToRead(@SuppressWarnings("rawtypes") RecordReader reader, PigSplit split)
			throws IOException {
		this.reader = (RecordReader<LongWritable, Text>)reader;
		// this.dmp = new diff_match_patch();
		this.tuples = TupleFactory.getInstance();
		this.bags = BagFactory.getInstance();
	}

	@Override
	public Tuple getNext() throws IOException {
		boolean hasNext;
		try {
			hasNext = reader.nextKeyValue();
			if (hasNext) {
				Text content = reader.getCurrentValue();
				return tuples.newTupleNoCopy(Arrays.asList(content.toString()));	
			}
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
		return null;
	}
	
	@Override
	public void setLocation(String loc, Job job) throws IOException {
		setInputPaths(job, loc);
	}
	
	@Override
	public String[] getPartitionKeys(String loc, Job job) throws IOException {
		setLocation(loc, job);
		return null;
	}

	@Override
	public ResourceSchema getSchema(String arg0, Job arg1) throws IOException {
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
	
	protected void defineSchema() throws FrontendException {
		// TODO: define schema here
	}
}
