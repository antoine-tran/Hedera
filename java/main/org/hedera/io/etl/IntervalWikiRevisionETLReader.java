/**
 * 
 */
package org.hedera.io.etl;

import java.io.IOException;

import org.apache.hadoop.io.DataOutputBuffer;
import org.hedera.io.WikipediaRevisionHeader;

/**
 * A Wiki
 * @author tuan
 *
 */
public class IntervalWikiRevisionETLReader<KEYIN, VALUEIN> extends
		DefaultWikiRevisionETLReader<KEYIN, VALUEIN> {

	@Override
	protected ETLExtractor<KEYIN, VALUEIN, WikipediaRevisionHeader> initializeExtractor() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected KEYIN initializeKey() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected void freeKey(KEYIN key) {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected VALUEIN initializeValue() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected void freeValue(VALUEIN value) {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected org.hedera.io.etl.WikiRevisionETLReader.Ack readToNextRevision(
			DataOutputBuffer buffer, WikipediaRevisionHeader meta)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

}
