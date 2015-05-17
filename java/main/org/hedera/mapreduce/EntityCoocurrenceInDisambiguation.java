/**
 * 
 */
package org.hedera.mapreduce;

import java.io.IOException;

import org.apache.commons.lang.WordUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.io.pair.PairOfStringInt;
import edu.umd.cloud9.io.pair.PairOfStrings;
import tuan.hadoop.conf.JobConfig;

/**
 * @author tuan
 *
 */
public class EntityCoocurrenceInDisambiguation extends JobConfig implements Tool {

	private static final String PHASE = "phase";

	// Input: Wikipedia page, output: a word in the title of the disambiguation, list of pages
	private static final class DisambMapper extends Mapper<LongWritable, WikipediaPage, PairOfStringInt, PairOfStrings> {
		private Text outKey = new Text();
		private PairOfStringInt outVal = new PairOfStringInt();

		@Override
		protected void map(LongWritable key, WikipediaPage p, Context context) 
				throws IOException, InterruptedException {

			// only articles are emitted
			boolean redirected = false;
			if (p.isRedirect()) {
				redirected = true;
			} else if (!p.isArticle())
				return;
			String title = p.getTitle().trim();

			// to make the title case-sensitive, we will change all lower-cased
			// first characters to upper-case.
			if (title.isEmpty())
				return;

			// do not pass the id message of a redirect article
			if (!redirected) {
				outKey.set(title);
				outVal.set(p.getDocid(), -1);
				context.write(outKey, outVal);
			}

			if (title.endsWith("(disambiguation)")) {
				int i = title.indexOf(" (disambiguation)");
				if (i < 0) {
					return;
				}
				outKey.set(title.substring(0, i));

				for (String t : p.extractLinkTargets()) {
					if (t.isEmpty()) {
						continue;
					}
				}	
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new EntityCoocurrenceInDisambiguation(), args);
		} catch (Exception e) {		
			e.printStackTrace();
		}
	}

}
