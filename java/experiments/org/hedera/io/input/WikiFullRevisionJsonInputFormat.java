/**
 * 
 */
package org.hedera.io.input;

import static org.hedera.io.input.WikiRevisionInputFormat.REVISION_BEGIN_TIME;
import static org.hedera.io.input.WikiRevisionInputFormat.REVISION_END_TIME;
import static org.hedera.io.input.WikiRevisionInputFormat.SKIP_NON_ARTICLES;

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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.hedera.io.FullRevision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Provide a converter of Json revisions to FullRevision object
 * 
 * The following code is inspired by the source code of
 * Manning book, "Hadoop in Practice", source:
 * https://github.com/alexholmes/hadoop-book 
 * 
 * @author tuan
 * @author alexholmes
 *
 */
public class WikiFullRevisionJsonInputFormat 
		extends FileInputFormat<LongWritable, FullRevision> {

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
	public RecordReader<LongWritable, FullRevision> createRecordReader(
			InputSplit input, TaskAttemptContext tac) throws IOException,
			InterruptedException {
		return new JsonRevisionReader();
	}

	public static class JsonRevisionReader 
	extends RecordReader<LongWritable, FullRevision> {
		private static final Logger LOG =
				LoggerFactory.getLogger(JsonRevisionReader
						.class);

		protected static long DEFAULT_MAX_BLOCK_SIZE = 134217728l;
		
		// option to whether skip non-article pages
		protected boolean skipNonArticles = true;
		
		// option to skip revisions outside a range
		protected long minTime = 0l;
		protected long maxTime = Long.MAX_VALUE;
		
		private LineRecordReader reader = new LineRecordReader();

		private final FullRevision value = new FullRevision();
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
		public FullRevision getCurrentValue() throws IOException,
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
				FullRevision value) {
			// LOG.info("Got string '{}'", line);
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
				
				String user = obj.get("user").getAsString();
				value.setUser(user);
				
				long userId = obj.get("user_id").getAsLong();
				value.setUserId(userId);
				
				String comment = obj.get("comment").getAsString();
				value.setComment(comment);
				
				byte[] text = obj.get("text").getAsString()
						.getBytes(StandardCharsets.UTF_8);
				value.loadText(text, 0, text.length);
										
				return true;
			} catch (Exception e) {
				LOG.warn("Could not json-decode string: " + line, e);
				return false;
			} 
		}
	}
}
