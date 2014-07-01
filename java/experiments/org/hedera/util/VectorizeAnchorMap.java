package org.hedera.util;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import me.lemire.integercompression.FastPFOR;
import me.lemire.integercompression.IntWrapper;
import me.lemire.integercompression.VariableByte;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.clueweb.dictionary.DefaultFrequencySortedDictionary;
import org.clueweb.util.AnalyzerFactory;

import edu.umd.cloud9.io.map.HMapSIW;

import tl.lin.data.array.IntArrayWritable;
import tl.lin.lucene.AnalyzerUtils;
import tuan.hadoop.conf.JobConfig;
import tuan.hadoop.io.IntArrayListWritable;

/** 
 * This tool reads the mapping of Wikipedia entity ID - anchor mapping
 * output from Cloud9, checks the continuous entity IDs and IDs of
 * anchor texts, and repacks everything into continuous id ranges
 */
public class VectorizeAnchorMap extends JobConfig implements Tool {

	private static final Logger LOG = Logger.getLogger(VectorizeAnchorMap.class);

	public static final String DICTIONARY_OPTION = "dictionary";
	public static final String PREPROCESSING = "preprocessing";

	private Analyzer analyzer;

	/** We convert the cloud9 anchor mappings into inverted-index style
	 * keyed by anchors as an array of integers. Values for each anchor
	 * is a list of entity ids */
	private final class MyMapper extends Mapper<IntWritable, HMapSIW, 
	IntArrayListWritable, IntArrayListWritable> {

		private final IntArrayListWritable anchorId = new IntArrayListWritable();
		private final IntArrayListWritable entities = new IntArrayListWritable();

		private DefaultFrequencySortedDictionary dictionary;

		@Override
		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			String path = conf.get(DICTIONARY_OPTION);
			dictionary = new DefaultFrequencySortedDictionary(path, fs);
			String analyzerType = conf.get(PREPROCESSING);
			analyzer = AnalyzerFactory.getAnalyzer(analyzerType);
			if (analyzer == null) {
				LOG.error("Error: proprocessing type not recognized. Abort " 
						+ this.getClass().getName());
				System.exit(1);
			}
			entities.set(new int[1], 1);
		}

		@Override
		protected void map(IntWritable key, HMapSIW value, Context context)
				throws IOException, InterruptedException {
			Set<String> anchors = value.keySet();
			IntArrayList lst = new IntArrayList();
			entities.set(0, key.get());
			for (String anchor : anchors) {
				List<String> tokens = AnalyzerUtils.parse(analyzer, anchor);
				lst.clear();
				boolean skipped = false;
				for (String t : tokens) {
					int id = dictionary.getId(t);
					if (id != -1) {
						lst.add(id);
					} 

					// we ignore anchor texts with strange tokens
					else {
						skipped = true;
					}
				}

				if (!skipped) {
					toIntArrayWritable(anchorId, lst.toIntArray(), lst.size());
					context.write(anchorId, entities);
				}
			}
		}
	}

	private final class MyReducer extends Reducer<IntArrayListWritable, IntArrayListWritable,
			IntArrayListWritable, IntArrayListWritable> {
		
		private final IntArrayListWritable keyOut = new IntArrayListWritable();
		private final IntArrayListWritable valOut = new IntArrayListWritable();
		
		@Override
		protected void reduce(IntArrayListWritable k,
				Iterable<IntArrayListWritable> v, Context context)
				throws IOException, InterruptedException {
			
			keyOut.set(k.toArray(), k.size());
			
			for (IntArrayListWritable entities : v) {
				entities.sort(true);
				if (valOut.size() == 0) {
					valOut.set(entities.toArray(), entities.size());
				}
				else for (int id : entities.toArray()) {
					int i = valOut.indexOf(id);
					if (i == -1) {
						valOut.add(id);
					}
				}
			}
		}
	}
	
	
	private static final FastPFOR P4 = new FastPFOR();
	private static final VariableByte VB = new VariableByte();

	public static void toIntArrayWritable(IntArrayListWritable ints, int[] termids, int length) {
		// Remember, the number of terms to serialize is length; the array might be longer.
		try {
			if (termids == null) {
				termids = new int[] {};
				length = 0;
			}

			IntWrapper inPos = new IntWrapper(0);
			IntWrapper outPos = new IntWrapper(1);

			int[] out = new int[length + 1];
			out[0] = length;

			if (length < 128) {
				VB.compress(termids, inPos, length, out, outPos);
				ints.set(out, outPos.get());

				return;
			}

			P4.compress(termids, inPos, (length/128)*128, out, outPos);

			if (length % 128 == 0) {
				ints.set(out, outPos.get());
				return;
			}

			VB.compress(termids, inPos, length % 128, out, outPos);
			ints.set(out, outPos.get());
		} catch (Exception e) {
			e.printStackTrace();
			ints.set(new int[] {}, 0);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new VectorizeAnchorMap(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
