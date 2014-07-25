package org.hedera.pig.load;

import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class TimeseriesLoader extends LoadFunc implements LoadMetadata {

	private TextInputFormat inputFormat = new TextInputFormat();
	private RecordReader<IntWritable, Text> reader;

	protected TupleFactory tuples;
	protected BagFactory bags;

	protected volatile ResourceSchema schema;

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

	@SuppressWarnings("rawtypes")
	@Override
	public void prepareToRead(RecordReader reader, PigSplit ps)
			throws IOException {
		this.reader = reader;	
		this.tuples = TupleFactory.getInstance();
		this.bags = BagFactory.getInstance();

	}

	@SuppressWarnings("rawtypes")
	@Override
	public InputFormat getInputFormat() throws IOException {
		return inputFormat;
	}

	public Tuple getNext() {
		return null;
	}

	// Define the Page schema (http://www.mediawiki.org/wiki/Manual:Page_table) in Pig. Some
	// fields are disabled: page_counter, page_random, page_touched, page_content_mode, 
	// page_restrictions, page_is_new.
	// Some fields are added:
	protected void defineSchema() throws FrontendException {
		Schema schema = new Schema();

		// canonical fields in Wikipedia SQL dump
		schema.add(new FieldSchema("entity", DataType.LONG));
		schema.add(new FieldSchema("timeseries", DataType.CHARARRAY));			

		// Added fields
		this.schema = new ResourceSchema(schema);
	}
}


