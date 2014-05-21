package org.hedera.pig.wikipedia.load;

import java.io.IOException;
import java.util.Arrays;

import static java.lang.String.valueOf;
import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths;

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

import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
import edu.umd.cloud9.collection.wikipedia.WikipediaPageInputFormat;

/**
 * This is a UDF loader that pipelines records in Wikipedia XML dump to Pig tuple, using
 * WikipediaPageInputFormat. It outputs a simple schema, which contains only columns defined
 * in Page schema (http://www.mediawiki.org/wiki/Manual:Page_table)
 * @author tuan
 */
public class LiteWikipediaLoader extends LoadFunc implements LoadMetadata {

	private static final WikipediaPageInputFormat INPUT_FORMAT = new WikipediaPageInputFormat();
	
	protected RecordReader<LongWritable, WikipediaPage> reader; 

	// a cached object that defines the output schema of a Wikipedia page. Use volatile to fix
	// the infamous double-checked locking issue, and to make access to this object thread-safe
	protected volatile ResourceSchema schema;
	
	protected TupleFactory tuples;
	protected BagFactory bags;
	
	@SuppressWarnings("rawtypes")
	@Override
	public InputFormat getInputFormat() throws IOException {
		return INPUT_FORMAT;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void prepareToRead(@SuppressWarnings("rawtypes") RecordReader reader, PigSplit split)
			throws IOException {
		this.reader = (RecordReader<LongWritable, WikipediaPage>)reader;	
		this.tuples = TupleFactory.getInstance();
		this.bags = BagFactory.getInstance();
	}

	@Override
	// read each block of Wikipedia page in XML format, convert them into relation following 
	// the layout of Wikipedia SQL dump
	public Tuple getNext() throws IOException {
		boolean hasNext;
		try {
			hasNext = reader.nextKeyValue();
			if (hasNext) {
				WikipediaPage page = reader.getCurrentValue();
				String id = page.getDocid();
				String title = page.getTitle();
				boolean isArticle = page.isArticle();
				boolean isDisamb = page.isDisambiguation();
				boolean isRedirect = page.isRedirect();
				String text = page.getWikiMarkup();
				if (text == null) return null;				
				String length = valueOf(text.length());
				Tuple tuple = tuples.newTupleNoCopy(Arrays.asList(id, (isArticle) ? "0" : "118", 
						title, text, valueOf(isRedirect), length, valueOf(isDisamb)));
				return tuple;
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

	// Define the Page schema (http://www.mediawiki.org/wiki/Manual:Page_table) in Pig. Some
	// fields are disabled: page_counter, page_random, page_touched, page_content_mode, 
	// page_restrictions, page_is_new.
	// Some fields are added:
	protected void defineSchema() throws FrontendException {
		Schema schema = new Schema();
		
		// canonical fields in Wikipedia SQL dump
		schema.add(new FieldSchema("page_id", DataType.LONG));
		schema.add(new FieldSchema("page_namespace", DataType.INTEGER));
		schema.add(new FieldSchema("page_title", DataType.CHARARRAY));
		schema.add(new FieldSchema("text", DataType.CHARARRAY));
		schema.add(new FieldSchema("page_is_redirect", DataType.BOOLEAN));
		schema.add(new FieldSchema("page_len", DataType.LONG));

		// Added fields
		schema.add(new FieldSchema("page_is_disamb", DataType.BOOLEAN));
		this.schema = new ResourceSchema(schema);
	}
}
