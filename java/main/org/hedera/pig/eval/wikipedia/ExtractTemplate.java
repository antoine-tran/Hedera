package org.hedera.pig.eval.wikipedia;

import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.hedera.pig.eval.PageFunc;
import org.mortbay.log.Log;


/**
 * Extract a list of templates from Wikipedia raw page and have it
 * returned as a data bag
 * @author tuan
 *
 */
public class ExtractTemplate extends PageFunc<DataBag> {

	private BagFactory bags = BagFactory.getInstance();
	private TupleFactory tuples = TupleFactory.getInstance();
		
	private static final Pattern[] NOT_TEMPLATE_PATTERN = new Pattern[] {
		Pattern.compile("R from\\s*.*", Pattern.DOTALL | Pattern.MULTILINE), 
		Pattern.compile("Redirect[\\s\\|].*", Pattern.DOTALL | Pattern.MULTILINE),
		Pattern.compile("Cite[\\s\\|].*", Pattern.DOTALL | Pattern.MULTILINE), 
		Pattern.compile("cite[\\s\\|].*", Pattern.DOTALL | Pattern.MULTILINE),
		Pattern.compile("Use[\\s\\|].*", Pattern.DOTALL | Pattern.MULTILINE), 
		Pattern.compile("pp\\-move\\-indef\\s*.*", Pattern.DOTALL | Pattern.MULTILINE), 
		Pattern.compile("File:[\\s\\|].*", Pattern.DOTALL | Pattern.MULTILINE), 
		Pattern.compile("Related articles[\\s\\|].*", Pattern.DOTALL | Pattern.MULTILINE),
		Pattern.compile("lang[\\s\\|].*", Pattern.DOTALL | Pattern.MULTILINE), 
		Pattern.compile("quote[\\s\\|].*", Pattern.DOTALL | Pattern.MULTILINE), 
		Pattern.compile("lang\\-en[\\s\\|].*", Pattern.DOTALL | Pattern.MULTILINE),
		Pattern.compile("LSJ[\\s\\|].*", Pattern.DOTALL | Pattern.MULTILINE), 
		Pattern.compile("OCLC[\\s\\|].*", Pattern.DOTALL | Pattern.MULTILINE),
		Pattern.compile("Main[\\s\\|].*|", Pattern.DOTALL | Pattern.MULTILINE), 
		Pattern.compile("IEP\\|.*", Pattern.DOTALL | Pattern.MULTILINE),
		Pattern.compile("sep entry[\\s\\|].*", Pattern.DOTALL | Pattern.MULTILINE), 
		Pattern.compile("Wayback[\\s\\|].*", Pattern.DOTALL | Pattern.MULTILINE),
		Pattern.compile("See also[\\s\\|].*", Pattern.DOTALL | Pattern.MULTILINE), 
		Pattern.compile("inconsistent citations.*", Pattern.DOTALL | Pattern.MULTILINE),
		// Harvard cite no brackets
		Pattern.compile("Harvnb[\\s\\|].*", Pattern.DOTALL | Pattern.MULTILINE), 
		Pattern.compile("Lookfrom[\\s\\|].*", Pattern.DOTALL | Pattern.MULTILINE), 
		Pattern.compile("Portal[\\s\\|].*", Pattern.DOTALL | Pattern.MULTILINE),
		Pattern.compile("Reflist[\\s\\|].*", Pattern.DOTALL | Pattern.MULTILINE), 
		Pattern.compile("Sister project links[\\s\\|].*", Pattern.DOTALL | Pattern.MULTILINE),
		Pattern.compile("Link[\\s\\|].*", Pattern.DOTALL | Pattern.MULTILINE), 
		Pattern.compile("link[\\s\\|].*", Pattern.DOTALL | Pattern.MULTILINE),

		// WikiProject BBC
		Pattern.compile("WikiProject[\\s\\|].*", Pattern.DOTALL | Pattern.MULTILINE), 
		Pattern.compile("BBCNAV[\\s\\|].*", Pattern.DOTALL | Pattern.MULTILINE),
		Pattern.compile("Wikipedia:WikiProject[\\s\\|]", Pattern.DOTALL | Pattern.MULTILINE), 
		Pattern.compile("User:Mollsmolyneux[\\s\\|].*", Pattern.DOTALL | Pattern.MULTILINE),
		Pattern.compile("subst:[\\s\\|].*", Pattern.DOTALL | Pattern.MULTILINE), 
		Pattern.compile("BBC[\\s\\|].*", Pattern.DOTALL | Pattern.MULTILINE),
		Pattern.compile("BBC\\-.*stub.*", Pattern.DOTALL | Pattern.MULTILINE)
	};
	
	private static final boolean isNotTemplateQuote(String title, String text) {
		String qtext = Pattern.quote(text); 
		for (Pattern p : NOT_TEMPLATE_PATTERN) {
			if (p.matcher(qtext).matches()) {
				return true;
			}
		}
		if (text.indexOf('|') > 0) {
			return true;
		}
		if (text.indexOf('\n') > 0) {
			return true;
		}
		if (text.endsWith("icon") || text.endsWith("sidebar")) {
			return true;
		}
		if (text.equalsIgnoreCase("good article") || text.equals("-")) {
			return true;
		}
		if (text.length() == 0) {
			return true;
		}
		if (text.indexOf(':') != -1) {
			return true;
		}
		
		// special markups for redirect
		if (text.equalsIgnoreCase("R from CamelCase")) {
			return true;
		}
		
		if (text.equalsIgnoreCase("refend")) {
			return true;
		}
		
		// Gotcha: A quick trick to avoid BBC player linkage 
		// e.g. ({{In Our Time|Anarchism|p0038x9t|Anarchism}}). 
		// Not work in all cases
		if (text.endsWith("|" + title)) {
			return true;
		}
		else return false;		
	}
	
	
	/* (non-Javadoc)
	 * @see org.apache.pig.EvalFunc#outputSchema(org.apache.pig.impl.logicalLayer.schema.Schema)
	 */
	@Override
	public Schema outputSchema(Schema inputSchema) {
		try {
			Schema template = new Schema();
			template.add(new FieldSchema("target", DataType.CHARARRAY));
			//template.add(new FieldSchema("anchor", DataType.CHARARRAY));
			FieldSchema tupleFs = new FieldSchema("tuple_of_templates", template, DataType.TUPLE);
			Schema tuple = new Schema(tupleFs);
			FieldSchema bagFs = new FieldSchema("bag", tuple, DataType.BAG);
			return new Schema(bagFs);
		} catch (Exception e) {
			Log.info("Error: ", e);
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public DataBag parse(long id, String title, String rawContent) {
					
		DataBag bag = bags.newDefaultBag();
		// bag.add(tuples.newTupleNoCopy(Arrays.asList(rawContent, "anchor")));
		
		int start = 0;
		// rawContent = rawContent.replace('\n', ' ');
		while (true) {
			start = rawContent.indexOf("{{", start);

			if (start < 0) {
				break;
			}
			int end = rawContent.indexOf("}}", start);
			if (end < 0) {
				break;
			}
			String text = rawContent.substring(start + 2, end);
			if (isNotTemplateQuote(title, text)) {
				start = end + 1;
				continue;
			}
			
			// if there is anchor text, get only article title
			int a;
			if ((a = text.indexOf("#")) != -1) {
				text = text.substring(0, a);
			}
			// ignore article-internal links, e.g., [[#section|here]]
			if (text.length() == 0) {
				start = end + 1;
				continue;
			}
			
			bag.add(tuples.newTupleNoCopy(Arrays.asList(text)));
			start = end + 1;
		}
		
		// return (bag.size() == 0) ? null : bag;
		return bag;
	}
	
	/*public static void main(String[] args) {
		StringBuilder sb = new StringBuilder();
		for (String line : FileUtility.readLines(args[0])) {
			sb.append(line + "\n");
		}
		System.out.println(isNotTemplateQuote("", sb.toString()));
	}*/
}
