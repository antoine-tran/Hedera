package org.hedera;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.hedera.io.RevisionBOW;
import org.hedera.io.RevisionHeader;
import org.hedera.io.etl.ETLExtractor;

public class LocalWikiRevisionBOWReader extends 
		LocalIntervalWikiRevisionETLReader<LongWritable, RevisionBOW> {

	private long unitInterval = 1000 * 60 * 60l;
	
	// maintain the word sequence of last visited revision
	private List<String> prevRevWords = new LinkedList<>();

	// id, timestamp, length
	private long[] prevRev = new long[3];

	@Override
	public void initialize() throws IOException {
		super.initialize();		
	}

	@Override
	protected void clearRevisions() {
		super.clearRevisions();
		prevRevWords.clear();
		prevRev[0] = prevRev[1] = prevRev[2] = 0l;
	}

	@Override
	protected ETLExtractor<LongWritable, RevisionBOW, 
	RevisionHeader> initializeExtractor() {
		return new LocalBOWExtractor(prevRevWords, prevRev, unitInterval);
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
	protected RevisionBOW initializeValue() {
		return new RevisionBOW();
	}

	@Override
	protected void freeValue(RevisionBOW value) {
		value.clear();
	}
}
