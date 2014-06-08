package org.hedera.io.etl;

import org.apache.hadoop.io.LongWritable;
import org.hedera.io.input.WikiRevisionInputFormat;

/** Input format that transforms a set of revisions within one unit interval
 *  into a bag of words that appear during the interval */
public class IntervalRevisionBOWInputFormat extends
		WikiRevisionInputFormat<LongWritable, VALUEIN> {

}
