package org.hedera.mapreduce;

import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.procedure.TObjectIntProcedure;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import tl.lin.data.pair.PairOfStrings;
import tuan.hadoop.conf.JobConfig;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by tuan on 16/09/15.
 */
public class BuildShortTermTS extends JobConfig implements Tool {
    private static final DateTimeFormatter dtf = DateTimeFormat.forPattern("YYYY-MM-dd");

    private static final String PHASE = "phase";
    private static final String PHASE_OPT = "phase.opt";

    // Step 1: flatten all destinations into one line
    private static class MyMapper extends Mapper<LongWritable, Text, PairOfStrings, Text> {

        private final PairOfStrings KEY = new PairOfStrings();
        private final Text VAL = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            int i = line.indexOf('\t');
            int j = line.indexOf('\t',i+1);
            int k = line.indexOf('\t',j+1);

            long timestamp = Long.parseLong(line.substring(j+1, k));
            DateTime dt = new DateTime(timestamp);

            KEY.set(line.substring(0, i), dt.toString(dtf));
            VAL.set(line.substring(i+1, j));

            context.write(KEY, VAL);
        }
    }

    private static class MyReducer extends Reducer<PairOfStrings, Text, PairOfStrings, IntWritable> {

        private final PairOfStrings KEY = new PairOfStrings();
        private final IntWritable VAL = new IntWritable();

        @Override
        protected void reduce(final PairOfStrings key, Iterable<Text> vals, final Context context)
                throws IOException, InterruptedException {

            TObjectIntHashMap<String> idx = new TObjectIntHashMap<>();
            for (Text val : vals) {
                idx.adjustOrPutValue(val.toString(), 1, 1);
            }

            // Now if we are not crashed yet, move on
            TObjectIntHashMap<String> mi = combinations(idx);
            if (mi == null) return;

            mi.forEachEntry(new TObjectIntProcedure<String>() {
                public boolean execute(String k, int v) {
                    try {
                        KEY.set(k, key.getRightElement());
                        VAL.set(v);
                        context.write(KEY, VAL);
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                    return true;
                }
            });
        }
    }

    @SuppressWarnings("static-access")
    @Override
    public Options options() {
        Options opts = super.options();
        Option pOpt = OptionBuilder.withArgName("phase").hasArg(true).isRequired(false)
                .withDescription("phase [aggr|cnt]").create(PHASE);
        opts.addOption(pOpt);
        return opts;
    }
    @Override
    public int run(String[] args) throws Exception {
        parseOtions(args);
        String phase = "aggr";

        if (command.hasOption(PHASE)) {
            phase = command.getOptionValue(PHASE);
        }

        if ("aggr".equals(phase)) {
            phase1(args);
        }
        else if ("cnt".equals(phase)) {
            phase2(args);
        }
        return 0;
    }

    private int phase1(String[] args) throws InterruptedException,
            IOException, ClassNotFoundException {
        System.out.println("Phase 1");
        Job job = setup(TextInputFormat.class, SequenceFileOutputFormat.class,
                PairOfStrings.class, Text.class,
                PairOfStrings.class, IntWritable.class,
                MyMapper.class, MyReducer.class, args);
        Configuration conf = job.getConfiguration();
        conf.set("mapreduce.map.memory.mb", "4096");
        conf.set("mapreduce.map.java.opts", "-Xmx4096m");
        conf.set("mapreduce.reduce.memory.mb", "4096");
        conf.set("mapreduce.reduce.java.opts", "-Xmx4096m");
        job.waitForCompletion(true);
        return 0;
    }

    private int phase2(String[] args) throws InterruptedException,
            IOException, ClassNotFoundException {
        System.out.println("Phase 1");
        Job job = setup(SequenceFileInputFormat.class, TextOutputFormat.class,
                PairOfStrings.class, IntWritable.class,
                PairOfStrings.class, IntWritable.class,
                Mapper.class, IntSumReducer.class, args);
        Configuration conf = job.getConfiguration();
        conf.set("mapreduce.map.memory.mb", "4096");
        conf.set("mapreduce.map.java.opts", "-Xmx4096m");
        conf.set("mapreduce.reduce.memory.mb", "4096");
        conf.set("mapreduce.reduce.java.opts", "-Xmx4096m");
        job.waitForCompletion(true);
        return 0;
    }

    private static TObjectIntHashMap<String> combinations(TObjectIntHashMap<String> maps) {
        String[] keys = maps.keys(new String[maps.size()]);
        Arrays.sort(keys);
        TObjectIntHashMap<String> results = new TObjectIntHashMap<>(maps.size() * (maps.size()-1)/2);

        for (int i=0; i<keys.length-1;i++) {
            for (int j=i+1; j<keys.length;j++) {
                if (keys[i].equals(keys[j])) continue;
                String k = keys[i] + "@@" + keys[j];
                results.adjustOrPutValue(k, Math.min(maps.get(keys[i]), maps.get(keys[j])),
                        Math.min(maps.get(keys[i]), maps.get(keys[j])));
            }
        }
        return (results.size() == 0) ? null : results;
    }

    public static void main(String[] args) {
        try {
            ToolRunner.run(new BuildShortTermTS(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
		/*DateTime dt = new DateTime(1418431632000l);
		System.out.println(dt.toString(dtf));*/
    }
}