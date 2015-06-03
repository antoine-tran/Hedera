package org.hedera.pig.eval.wikipedia;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.google.common.net.InternetDomainName;

public class ExtractDomain extends EvalFunc<String> {

	@Override
	public String exec(Tuple input) throws IOException {
		if (input == null || input.size() < 1) {
			return null;
		}
		String url = DataType.toString(input.get(0));
		try {
			return getDomainName(url);
		} catch (URISyntaxException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	private static String getDomainName(String u) throws URISyntaxException {
		URI uri = new URI(u);
		String hostname = uri.getHost();
		InternetDomainName idn =  InternetDomainName.from(hostname);
		String sld = idn.topPrivateDomain().name();
		String tld = idn.publicSuffix().name();
		return sld + "\t" + tld;
	}
	
	@Override
	public Schema outputSchema(Schema input) {

		return new Schema(new Schema.FieldSchema(getSchemaName(
				this.getClass().getName().toLowerCase(), input), 
				DataType.CHARARRAY));
	}

	@Override

	public List getArgToFuncMapping() throws FrontendException {

		List funcList = new ArrayList();

		funcList.add(new FuncSpec(this.getClass().getName(), new Schema(new Schema.FieldSchema(null, 
				DataType.CHARARRAY))));

		return funcList;

	}
	
	public static void main(String[] args) throws URISyntaxException {
		URI uri = new URI("http://docs.guava-libraries.googlecode.com/git/javadoc/com/google/common/net/InternetDomainName.html");
		
		System.out.println();
	}
}
