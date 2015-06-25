package org.hedera.mapreduce;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
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
 * Get the maximal timestamp in the revision history dataset
 * Created by tuan on 25/06/15.
 */
public class MaxTimestamp extends JobConfig implements Tool {

    private static class MyMapper extends Mapper<LongWritable, Text,
            LongWritable, LongWritable> {

        private final LongWritable ONE = new LongWritable(1);
        private static final LongWritable VAL_OUT = new LongWritable(1);
        private JsonParser parser;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            parser = new JsonParser();
        }

        @Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
            parser = null;
        }
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            JsonObject obj =
                    (JsonObject) parser.parse(value.toString());
            long timestamp = obj.get("timestamp").getAsLong();
            VAL_OUT.set(timestamp);
            context.write(ONE,VAL_OUT);
        }
    }

    private static class MyCombiner extends Reducer<LongWritable, LongWritable,
            LongWritable, LongWritable> {

        private final LongWritable VAL_OUT = new LongWritable(0l);

        @Override
        protected void reduce(LongWritable key, Iterable<LongWritable> values,
                              Context context) throws IOException,
                InterruptedException {
            long max = 0l;
            for (LongWritable v : values) {
                long val = v.get();
                if (val > max) {
                    max = val;
                }
            }
            VAL_OUT.set(max);
            context.write(key,VAL_OUT);
        }
    }

    private static class MyReducer extends Reducer<LongWritable, LongWritable,
            NullWritable, NullWritable> {

        private final LongWritable VAL_OUT = new LongWritable(0l);

        @Override
        protected void reduce(LongWritable key, Iterable<LongWritable> values,
                              Context context) throws IOException,
                InterruptedException {
            long max = 0l;
            for (LongWritable v : values) {
                long val = v.get();
                if (val > max) {
                    max = val;
                }
            }
            context.getCounter("org.hedera","maxts").setValue(max);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = setup(TextInputFormat.class, NullOutputFormat.class,
                LongWritable.class, LongWritable.class,
                NullWritable.class, NullWritable.class,
                MyMapper.class, MyReducer.class,MyCombiner.class, args);
        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) {
        try {
            ToolRunner.run(new MaxTimestamp(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
