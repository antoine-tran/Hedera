package org.hedera.io.etl;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.hedera.io.WikipediaHeader;
import org.hedera.io.WikipediaRevisionBOW;
import org.hedera.io.WikipediaRevisionHeader;

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
public class IntervalWikiRevisionBOWReader extends
		IntervalWikiRevisionETLReader<LongWritable, WikipediaRevisionBOW> {

	@Override
	protected ETLExtractor<LongWritable, WikipediaRevisionBOW, 
			WikipediaRevisionHeader> initializeExtractor() {
		return new WikiRevisionBOWExtractor();
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
	protected WikipediaRevisionBOW initializeValue() {
		return new WikipediaRevisionBOW();
	}

	@Override
	protected void freeValue(WikipediaRevisionBOW value) {
		value.clear();
	}

	@Override
	protected void processMetaData(DataOutputBuffer buffer, 
			WikipediaHeader meta) {
		// No op
	}
}
