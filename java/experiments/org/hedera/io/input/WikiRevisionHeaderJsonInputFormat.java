package org.hedera.io.input;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.hedera.io.RevisionHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/** 
 * A variant of WikiRevisionHeaderInputFormat that 
 * works with the Json dumps 
 * @author tuan
 */
public class WikiRevisionHeaderJsonInputFormat 
extends WikiRevisionInputFormat<LongWritable, RevisionHeader> {

	protected CompressionCodecFactory compressionCodecs = null;

	public void configure(Configuration conf) {
		if (compressionCodecs == null)
			compressionCodecs = new CompressionCodecFactory(conf);
	}

	@Override
	public boolean isSplitable(JobContext context, Path file) {
		Configuration conf = context.getConfiguration();
		configure(conf);
		CompressionCodec codec = compressionCodecs.getCodec(file);		
		if (null == codec) {
			return true;
		}
		return codec instanceof SplittableCompressionCodec;
	}

	@Override
	public RecordReader<LongWritable, RevisionHeader> createRecordReader(
			InputSplit input, TaskAttemptContext context)
					throws IOException, InterruptedException {
		return new JsonRevisionHeaderReader();
	}
	
	public static class JsonRevisionHeaderReader 
			extends RecordReader<LongWritable, RevisionHeader> {

		private static final Logger LOG =
				LoggerFactory.getLogger(JsonRevisionHeaderReader
						.class);

		protected static long DEFAULT_MAX_BLOCK_SIZE = 134217728l;
		
		// option to whether skip non-article pages
		protected boolean skipNonArticles = true;
		
		// option to skip revisions outside a range
		protected long minTime = 0l;
		protected long maxTime = Long.MAX_VALUE;
		
		private LineRecordReader reader = new LineRecordReader();

		private final RevisionHeader value = new RevisionHeader();
		private final JsonParser parser = new JsonParser();

		@Override
		public void initialize(InputSplit split,
				TaskAttemptContext context)
						throws IOException, InterruptedException {
			reader.initialize(split, context);
			Configuration conf = context.getConfiguration();
			conf.setLong("mapreduce.input.fileinputformat.split.maxsize", 
					DEFAULT_MAX_BLOCK_SIZE);
			skipNonArticles = conf.getBoolean(SKIP_NON_ARTICLES, true);
			minTime = conf.getLong(REVISION_BEGIN_TIME, 0);
			maxTime = conf.getLong(REVISION_END_TIME, Long.MAX_VALUE);
		}

		@Override
		public synchronized void close() throws IOException {
			reader.close();
		}

		@Override
		public LongWritable getCurrentKey() throws IOException,
		InterruptedException {
			return reader.getCurrentKey();
		}

		@Override
		public RevisionHeader getCurrentValue() throws IOException,
		InterruptedException {
			return value;
		}

		@Override
		public float getProgress()
				throws IOException, InterruptedException {
			return reader.getProgress();
		}

		@Override
		public boolean nextKeyValue()
				throws IOException, InterruptedException {
			while (reader.nextKeyValue()) {
				value.clear();
				if (decodeLineToJson(parser, reader.getCurrentValue(),
						value)) {
					return true;
				}
			}
			return false;
		}

		public boolean decodeLineToJson(JsonParser parser, Text line,
				RevisionHeader value) {
			
			try {
				JsonObject obj =
						(JsonObject) parser.parse(line.toString());
				
				int namespace = obj.get("page_namespace").getAsInt();
				if (namespace != 0 && skipNonArticles) {
					return false;
				}
				value.setNamespace(namespace);

				long pageId = obj.get("page_id").getAsLong();
				value.setPageId(pageId);
				
				String title = obj.get("page_title").getAsString();
				value.setPageTitle(title);
				
				long revId = obj.get("rev_id").getAsLong();
				value.setRevisionId(revId);
				
				long parentId = obj.get("parent_id").getAsLong();
				value.setParentId(parentId);
				
				long timestamp = obj.get("timestamp").getAsLong();
				value.setTimestamp(timestamp);
													
				return true;
			} catch (Exception e) {
				LOG.warn("Could not json-decode string: " + line, e);
				return false;
			} 
		}
		
	}
}


