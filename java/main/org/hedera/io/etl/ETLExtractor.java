package org.hedera.io.etl;

import org.apache.hadoop.io.DataOutputBuffer;


/** API to provide algorithms for extracting information right in readers */
public interface ETLExtractor<KEY, VALUE, META> {

	/** compare two revisions based on their meta-data */
	public float check(META metaNow, META metaBefore);
	
	/** extract the revision content and populate the OUTPUT .
	 * 
	 * NOTE: Make sure all the time and value is clean before, when
	 * the revisions are treated independently */
	public void extract(DataOutputBuffer content, META meta, KEY key, VALUE value);	
}
