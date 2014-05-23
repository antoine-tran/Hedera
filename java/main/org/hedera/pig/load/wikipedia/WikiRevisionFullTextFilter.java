/**
 * 
 */
package org.hedera.pig.load.wikipedia;

import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths;

import java.io.IOException;

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
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.hedera.mapreduce.io.wikipedia.WikipediaRevisionInputFormat;

/**
 * A Pig UDF loader that filters wiki revision text by keywords
 * @author tuan
 *
 */
public class WikiRevisionFullTextFilter extends LoadFunc implements LoadMetadata {

	private WikipediaRevisionInputFormat input;
	
	// a cached object that defines the output schema of a Wikipedia revision
	// Use volatile to fix the infamous double-checked locking issue, 
	// and to make access to this object thread-safe
	protected volatile ResourceSchema schema;
	
	private RecordReader<LongWritable, Text> reader;
	
	protected TupleFactory tuples;
	protected BagFactory bags;
	
	public WikiRevisionFullTextFilter(String optString) {
		input = new WikipediaRevisionInputFormat(optString);
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

	@Override
	public InputFormat getInputFormat() throws IOException {
		return input;
	}
	
	private void defineSchema() {
		// TODO: define schema here
	}
	
	@Override
	public void prepareToRead(RecordReader reader, PigSplit split)
			throws IOException {
		this.reader = (RecordReader<LongWritable, Text>)reader;
	}

	@Override
	public Tuple getNext() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}


}
