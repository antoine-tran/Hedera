package org.hedera.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.solr.hadoop.SolrMapper;
import org.hedera.io.WikiRevisionWritable;

import tuan.hadoop.conf.JobConfig;

/**
 * This job reads the Wikipedia revision and builds the inverted index.
 * Format of  
 * @author tuan
 *
 */
public class BuildVersionedInvertedIndex extends JobConfig {

	private static final class IndexMapper extends SolrMapper<LongWritable, WikiRevisionWritable> {
		
	}
}
