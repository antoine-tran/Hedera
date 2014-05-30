package org.hedera.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.hedera.io.LongLongStringWritable;
import org.hedera.io.WikipediaRevision;

import edu.umd.cloud9.io.pair.PairOfStringInt;
import tuan.hadoop.conf.JobConfig;

/**
 * This jobs extract anchor text from Wikipedia revisions 
 * @author tuan
 *
 */
public class ExtractTemporalAnchorText extends JobConfig implements Tool {

	private static final class MyMapper extends Mapper<LongWritable, WikipediaRevision, 
			PairOfStringInt, LongLongStringWritable> {}
	
	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}
	
}
