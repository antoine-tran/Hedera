package org.hedera.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.util.Tool;
import org.hedera.io.LongLongStringWritable;
import org.hedera.io.WikipediaRevision;

import edu.umd.cloud9.io.pair.PairOfStringInt;
import tuan.hadoop.conf.JobConfig;

/**
 * This jobs extract temporal anchor text from Wikipedia revisions 
 * @author tuan
 *
 */
public class ExtractTemporalAnchorText extends JobConfig implements Tool {

	// Algorithm:
	// emit (title, 0) --> (doc id, timestamp, timfor structure message (
	private static final class MyMapper extends Mapper<LongWritable, WikipediaRevision, 
			PairOfStringInt, LongLongStringWritable> {

		private PairOfStringInt keyOut = new PairOfStringInt();
		private WikipediaRevision valOut = new WikipediaRevision();
		
		@Override
		protected void map(LongWritable key, WikipediaRevision value,
				Context context) throws IOException, InterruptedException {
			
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}
	
}
