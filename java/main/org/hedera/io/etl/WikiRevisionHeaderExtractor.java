package org.hedera.io.etl;

import org.apache.hadoop.io.DataOutputBuffer;
import org.hedera.io.WikipediaRevisionHeader;

import edu.umd.cloud9.io.pair.PairOfLongString;
import edu.umd.cloud9.io.pair.PairOfLongs;

public class WikiRevisionHeaderExtractor implements
		ETLExtractor<PairOfLongString, PairOfLongs, 
		WikipediaRevisionHeader> {

	@Override
	// We keep everything for articles
	public float check(WikipediaRevisionHeader meta1,
			WikipediaRevisionHeader meta2) {
		return 1f;
	}

	@Override
	public void extract(DataOutputBuffer content, WikipediaRevisionHeader meta,
			PairOfLongString key, PairOfLongs value) {
		long revId = meta.getRevisionId();
		long ts = meta.getTimestamp();
		
		key.set(meta.getPageId(), meta.getPageTitle());
		value.set(revId, ts);
	}
}
