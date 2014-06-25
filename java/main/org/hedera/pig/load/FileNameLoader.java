package org.hedera.pig.load;

import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths;

import java.io.IOException;
import java.util.Arrays;

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
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.hedera.io.input.FileNullInputFormat;
import org.hedera.io.input.FileNullInputFormat.FileNullRecordReader;

/**
 * A simple UDF loader that reads a file and returns its path
 */
public class FileNameLoader extends LoadFunc implements LoadMetadata {

	private boolean processed;
	// a cached object that defines the output schema of a Wikipedia page. Use volatile to fix
	// the infamous double-checked locking issue, and to make access to this object thread-safe
	protected volatile ResourceSchema schema;
	protected TupleFactory tuples;
	protected FileNullRecordReader reader;

	@Override
	public InputFormat getInputFormat() throws IOException {
		return new FileNullInputFormat();
	}

	@Override
	public Tuple getNext() throws IOException {
		if (!processed) {
			Text text = null;
			try {
				text = reader.getCurrentKey();
			} catch (InterruptedException e) {
				throw new IOException(e);
			}
			if (text != null) {
				Tuple tuple = tuples.newTupleNoCopy(Arrays.asList(text.toString()));
				return tuple;
			}
			return null;
		}
		return null;
	}

	@Override
	public void prepareToRead(RecordReader reader, PigSplit split)
			throws IOException {
		processed = false;
		this.reader = (FileNullRecordReader) reader;
		this.tuples = TupleFactory.getInstance();

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
	public ResourceStatistics getStatistics(String loc, Job job)
			throws IOException {
		return null;
	}

	@Override
	public void setPartitionFilter(Expression e) throws IOException {
	}

	protected void defineSchema() throws FrontendException {
		Schema schema = new Schema();

		// canonical fields in Wikipedia SQL dump
		schema.add(new FieldSchema("filename", DataType.CHARARRAY));
		this.schema = new ResourceSchema(schema);
	}

}
