package org.hedera.io.etl;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.hedera.io.WikipediaRevisionBOW;
import org.hedera.io.WikipediaRevisionHeader;

/**
 * This extractor compares the last revision with the current one, and updates
 * the patch of the BOW object accordingly
 * 
 * @author tuan
 *
 */
public class WikiRevisionBOWExtractor implements ETLExtractor<LongWritable,
		WikipediaRevisionBOW, WikipediaRevisionHeader> {

	@Override
	public float check(WikipediaRevisionHeader metaNow,
			WikipediaRevisionHeader metaBefore) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void extract(DataOutputBuffer content, WikipediaRevisionHeader meta,
			LongWritable key, WikipediaRevisionBOW value) {
		// TODO Auto-generated method stub
	}

}
