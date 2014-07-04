package org.hedera.graph;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hedera.io.FullRevision;

import tuan.hadoop.conf.JobConfig;

/**
 * 
 * This tools builds the (entity, doc) mapping based on explicit links
 * 1. Input: seed words / queries
 * 2. Dictionary
 * 3. Entity title - id mapping
 * 
 * Algorithm: 
 * a) Emit targets:
 * (key=offset, value=FullRevision) --> (key=(anchor text,title), value=(docId, revId, context))
 * 
 * b) Emit structure:
 * (key=offset, value)
 * 
 * @author tuan
 */
public class LinkEntityDocAnnot extends JobConfig implements Tool {

	
	/*private static final class MyMapper extends Mapper<LongWritable, FullRevision> {
		
	}*/
	
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}
	
	public static void main(String[] args) {
		try {
			ToolRunner.run(new LinkEntityDocAnnot(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
