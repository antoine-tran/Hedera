package org.hedera.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import tuan.hadoop.conf.JobConfig;

import java.io.IOException;

/**
 * Count the number of unique anchors
 * Created by tuan on 22/06/15.
 */
public class CountAnchor extends JobConfig implements Tool {

    private static class MyMapper extends Mapper<LongWritable, Text,
                Text, LongWritable> {

        private final Text KEY_OUT = new Text();
        private static final LongWritable VAL_OUT = new LongWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] cols = line.split("\t");
            KEY_OUT.set(cols[cols.length - 1]);
            context.write(KEY_OUT,VAL_OUT);
        }
    }

    private static class MyReducer extends Reducer<Text, LongWritable,
                NullWritable, NullWritable> {

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values,
                              Context context) throws IOException,
                InterruptedException {

            context.getCounter("org.hedera.wikipedia","anchorcount").increment(1);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = setup(TextInputFormat.class, NullOutputFormat.class,
                Text.class, LongWritable.class,
                NullWritable.class, NullWritable.class,
                MyMapper.class, MyReducer.class,args);
        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) {
        try {
            ToolRunner.run(new CountAnchor(),args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
