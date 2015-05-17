package org.hedera.pig.eval;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

/**
 * A custom eval function that wraps the Pig's eval func
 * and allows user to handle the raw content of page that
 * has id, title and content
 */
public abstract class PageFunc<T> extends EvalFunc<T> {

	@Override	
	public final T exec(Tuple tuple) throws IOException {
		try {
		T t = parse(Long.parseLong((String) tuple.get(0)), (String)tuple.get(1), 
				(String)tuple.get(2));
		return t;
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException(e);
		}
	}
	
	public abstract T parse(long pageId, String pageTitle, String pageContent);
}
