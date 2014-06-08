package org.hedera.io.etl;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.hedera.io.WikipediaRevisionHeader;

import edu.umd.cloud9.io.pair.PairOfLongString;
import edu.umd.cloud9.io.pair.PairOfLongs;

public class WikiRevisionHeaderExtractor implements
		ETLExtractor<LongWritable, PairOfLongs, 
		WikipediaRevisionHeader> {

	@Override
	// We keep everything for articles
	public float check(WikipediaRevisionHeader meta1,
			WikipediaRevisionHeader meta2) {
		return 1f;
	}

	@Override
	public void extract(DataOutputBuffer content, WikipediaRevisionHeader meta,
			LongWritable key, PairOfLongs value) {
		long revId = meta.getRevisionId();
		long ts = meta.getTimestamp();
		
		key.set(meta.getPageId());
		value.set(revId, ts);
	}
}