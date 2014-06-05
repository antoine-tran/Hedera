package org.hedera.io.etl;

import org.apache.hadoop.io.DataOutputBuffer;


/** API to provide algorithms for extracting information right in readers */
public interface ETLExtractor<KEY, VALUE, META> {

	/** compare two revisions based on their meta-data */
	public float check(META meta1, META meta2);
	
	/** extract the revision content and populate the OUTPUT */
	public void extract(DataOutputBuffer content, META meta, KEY key, VALUE value);	
}
