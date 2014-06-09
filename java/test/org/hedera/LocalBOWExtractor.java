package org.hedera;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.hedera.io.RevisionBOW;
import org.hedera.io.RevisionHeader;
import org.hedera.io.etl.ETLExtractor;
import org.hedera.util.MediaWikiProcessor;

import static org.hedera.io.input.WikiRevisionInputFormat.END_TEXT;

import difflib.Delta;
import difflib.DiffUtils;
import difflib.Patch;

public class LocalBOWExtractor implements ETLExtractor<LongWritable,
		RevisionBOW, RevisionHeader> {

	private List<String> prevRevWords;
	private long[] prevRev;
	private long unitInterval;
	
	private MediaWikiProcessor processor;

	public LocalBOWExtractor() {
		super();
		processor = new MediaWikiProcessor();
	}

	public LocalBOWExtractor(List<String> prevRevWords, long[] prevRev, long unitInterval) {
		this();
		this.prevRevWords = prevRevWords;
		this.prevRev = prevRev;
		this.unitInterval = unitInterval;
	}

	@Override
	public float check(RevisionHeader metaNow, RevisionHeader metaBefore) {
		if (metaBefore == null || metaBefore.getLength() == 0) return 1f;
		long tsNow = metaNow.getTimestamp();

		// defer assigning the first revision to after the extraction phase
		if (prevRev[1] == 0) {
			if (metaBefore == null || metaBefore.getLength() == 0) return 1f;
			if (metaNow.isMinor()) return 0.0005f;
			return Math.abs(metaNow.getLength() - metaBefore.getLength()) 	
					/ (float)metaBefore.getLength();
		}

		if (tsNow - prevRev[1] <= unitInterval) {
			return 0.0005f;
		}
		return Math.abs(metaNow.getLength() - prevRev[2]) 	
				/ (float)metaBefore.getLength();	
	}

	@Override
	public boolean extract(DataOutputBuffer content, RevisionHeader meta,
			LongWritable key, RevisionBOW value) {
		
		if (meta == null || meta.getLength() == 0) {
			return false;
		}
		// save headers
		key.set(meta.getPageId());
		value.setPageId(meta.getPageId());
		value.setNamespace(meta.getNamespace());
		value.setRevisionId(meta.getRevisionId());
		value.setTimestamp(meta.getTimestamp());
		
		if (prevRev[1] != 0) {
			value.setLastRevisionId(prevRev[0]);
			value.setLastTimestamp(prevRev[1]);
		}
	
		// remove mark-ups
		String rawText = new String(content.getData(), 0, content.getLength()
				- END_TEXT.length);
		String plainText = processor.getContent(rawText);
		List<String> thisRevWords = Arrays.asList(plainText.split("\\s+"));
		
		// apply diff algorithm here get the differences
		if (prevRevWords.isEmpty()) {
			value.buildBOW(thisRevWords);
		}
		else {
			Patch patch = DiffUtils.diff(prevRevWords, thisRevWords);
			for (Delta d : patch.getDeltas()) {
				for (Object s : d.getRevised().getLines()) {
					value.updateBOW((String) s);
				}
			} 
		}

		// shift revision to the new one
		prevRevWords.clear();
		prevRevWords.addAll(thisRevWords);
		prevRev[0] = meta.getRevisionId();
		prevRev[1] = meta.getTimestamp();
		prevRev[2] = meta.getLength();		
		
		return true;
	}
}

