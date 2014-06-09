package org.hedera.io.etl;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.hedera.io.RevisionConcatText;
import org.hedera.io.RevisionHeader;
import org.hedera.io.input.WikiRevisionInputFormat;

import difflib.Delta;
import difflib.DiffUtils;
import difflib.Patch;

public class RevisionConcatInputFormat extends
		WikiRevisionInputFormat<LongWritable, RevisionConcatText> {
	
	// default: 1 hour
	private long unitInterval = 1000 * 60 * 60;
	
	@Override
	public RecordReader<LongWritable, RevisionConcatText> createRecordReader(
			InputSplit input, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new IntervalRevisionConcatTextReader();
	}

	/**
	 * An ETL Reader that reads all revisions of a page within one specific interval,
	 * and generates a single Writable object that represents the concatenated bag
	 * of words of the page during the entire interval. To avoid calculating too many
	 * differentials between revisions, the interval unit is used (hour, day, week, etc.),
	 * and only the first and last revisions in one time unit interval is compared. 
	 * The comparison is also skipped if the length in bytes between two revisions is
	 * negligible.
	 *  
	 * @author tuan
	 * */
	public class IntervalRevisionConcatTextReader extends
			IntervalRevisionETLReader<LongWritable, RevisionConcatText> {
						
		@Override
		public void initialize(InputSplit input, TaskAttemptContext tac)
				throws IOException, InterruptedException {
			super.initialize(input, tac);
			Configuration conf = tac.getConfiguration();
			String scale = conf.get(SCALE_OPT);
			if (scale != null) {
				if (HOUR_SCALE_OPT.equals(scale)) {
					unitInterval = 1000 * 60 * 60;
				}
				else if (DAY_SCALE_OPT.equals(scale)) {
					unitInterval = 1000 * 60 * 60 * 24;
				}
				else if (WEEK_SCALE_OPT.equals(scale)) {
					unitInterval = 1000 * 60 * 60 * 24 * 7;
				}
				else if (MONTH_SCALE_OPT.equals(scale)) {
					unitInterval = 1000 * 60 * 60 * 24 * 30;
				}
			}
		}

		@Override
		protected ETLExtractor<LongWritable, RevisionConcatText, 
				RevisionHeader> initializeExtractor() {
			return new RevisionConcatTextExtractor();
		}
		
		@Override
		protected LongWritable initializeKey() {
			return new LongWritable();
		}

		@Override
		protected void freeKey(LongWritable key) {
			key.set(0);
		}

		@Override
		protected RevisionConcatText initializeValue() {
			return new RevisionConcatText();
		}

		@Override
		protected void freeValue(RevisionConcatText value) {
			value.clear();
		}

		@Override
		protected void processMetaData(DataOutputBuffer buffer, 
				RevisionHeader meta) {
			// No op
		}
	}
	
	/**
	 * This extractor compares the last revision with the current one, and updates
	 * the patch of the BOW object accordingly
	 * 
	 * @author tuan
	 *
	 */
	public class RevisionConcatTextExtractor implements ETLExtractor<LongWritable,
			RevisionConcatText, RevisionHeader> {
				
		@Override
		public float check(RevisionHeader metaNow,
				RevisionHeader metaBefore) {
			
			if (metaBefore == null || metaBefore.getLength() == 0) return 1f;
			long tsNow = metaNow.getTimestamp();
			long tsBefore = metaBefore.getTimestamp();
			
			// TODO: Wrong !! should use a global timestamp, tsBefore is
			// always updated 
			if (tsNow - tsBefore <= unitInterval) {
				return 0.0005f;
			}
			if (metaNow.isMinor()) return 0.0005f;
			return (metaNow.getLength() - metaBefore.getLength()) 	
					/ metaBefore.getLength();	
		}

		@Override
		public void extract(DataOutputBuffer content, RevisionHeader meta,
				LongWritable key, RevisionConcatText value) {
			
			List<String> lastText = value.getLastRevision();
			if (lastText == null) {
				
				// add meta-data		
				key.set(meta.getPageId());
				
				value.setNamespace(meta.getNamespace());
				value.setPageId(meta.getPageId());
				value.setPageTitle(meta.getPageTitle());
				value.setParentId(meta.getParentId());
				value.setRevisionId(meta.getRevisionId());
				value.setTimestamp(meta.getRevisionId());
				
				value.loadText(content.getData(), 0, content.getLength() 
						- END_TEXT.length);
			}
			else {
				// instantiate big objects
				String curRev = new String(content.getData(), 0, content.getLength() 
						- END_TEXT.length, StandardCharsets.UTF_8);
				
				String[] tokens = curRev.split("\\s+");
				List<String> curText = Arrays.asList(tokens);
				
				if (lastText.isEmpty()) {
					lastText = Arrays.asList(new String(value.getText(), 
							StandardCharsets.UTF_8).split("\\s+"));
				}
				
				Patch patch = DiffUtils.diff(lastText, curText);
				for (Delta d : patch.getDeltas()) {
					for (Object s : d.getRevised().getLines()) {
						value.addPatch((String) s);
					}
				}
				lastText = curText;
			}
		}
	}
}
