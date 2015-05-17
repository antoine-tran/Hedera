package org.hedera.pig.load;

import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.hedera.io.FullRevision;
import org.hedera.io.input.WikiRevisionFullInputFormat;
import org.hedera.io.input.WikiRevisionInputFormat;

public class WikiRevisionLoader extends LoadFunc implements LoadMetadata {
	private static final WikiRevisionInputFormat<LongWritable, FullRevision> INPUT_FORMAT = 
			new WikiRevisionFullInputFormat();

	protected RecordReader<LongWritable, FullRevision> reader;

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
		this.reader = reader;
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
				FullRevision content = reader.getCurrentValue();

				long pageId = content.getPageId();
				long revId = content.getRevisionId();
				long parentId = content.getParentId();
				long ts = content.getTimestamp();
				String user = content.getUser();
				long userId = content.getUserId();
				String title = content.getPageTitle();
				String comment = content.getComment();
				int ns = content.getNamespace();
				String text = new String(content.getText(), StandardCharsets.UTF_8);

				return tuples.newTupleNoCopy(Arrays.asList(
						pageId, title, ns, revId, parentId, 
						ts, user, userId, comment, text));	
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
		Schema schema = new Schema();

		schema.add(new FieldSchema("page_id", DataType.LONG));
		schema.add(new FieldSchema("page_title", DataType.CHARARRAY));
		schema.add(new FieldSchema("page_namespace", DataType.INTEGER));
		schema.add(new FieldSchema("rev_id", DataType.LONG));
		schema.add(new FieldSchema("parent_id", DataType.LONG));
		schema.add(new FieldSchema("timestamp", DataType.LONG));
		schema.add(new FieldSchema("user", DataType.CHARARRAY));
		schema.add(new FieldSchema("user_id", DataType.LONG));
		schema.add(new FieldSchema("comment", DataType.CHARARRAY));
		schema.add(new FieldSchema("text", DataType.CHARARRAY));

		this.schema = new ResourceSchema(schema);
	}
}
