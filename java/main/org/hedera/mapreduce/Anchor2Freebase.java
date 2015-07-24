/**
 * 
 */
package org.hedera.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import tuan.hadoop.conf.JobConfig;

/**
 * @author tuan
 *
 */
public class Anchor2Freebase extends JobConfig implements Tool {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			ToolRunner.run(new Anchor2Freebase(), args);
		} catch (Exception e) {			
			e.printStackTrace();
		}

	}
	
	private static final class MyMapper extends Mapper<LongWritable, Text, 
			Text, Text> {

		private final Text KEY = new Text();
		private final Text VAL = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			i = 
		}				
	}

	@Override
	public int run(String[] args) throws Exception {
		
		return 0;
	}

}
