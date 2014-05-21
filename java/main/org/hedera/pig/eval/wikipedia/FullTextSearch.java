/**
 * 
 */
package org.hedera.pig.eval.wikipedia;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.hedera.pig.eval.PageFunc;

/**
 * scan page's content and tells whether any of the specified keywowrds is in
 * 
 * @author tuan
 *
 */
public class FullTextSearch extends EvalFunc<Boolean> {

	@Override
	// Input includes the content of the page, and the list of keywords separated by TAB
	public Boolean exec(Tuple input) throws IOException {
		return null;
	}

}
