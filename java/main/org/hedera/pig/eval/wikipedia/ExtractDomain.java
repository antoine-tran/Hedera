package org.hedera.pig.eval.wikipedia;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
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
	
	private static String getDomainName(String u) throws
			URISyntaxException, UnsupportedEncodingException {
		URL url;
		try {
			url = new URL(u);
		} catch (MalformedURLException e) {
			e.printStackTrace();
			return null;
		}
		
		String path = url.getPath();
		if (path != null)
		  path = URLDecoder.decode(path, "UTF-8");
		String query = url.getQuery();
		if (query != null)
		  query = URLDecoder.decode(query, "UTF-8");
		String fragment = url.getRef();
		if (fragment != null)
		  fragment = URLDecoder.decode(fragment, "UTF-8");

		URI uri = new URI(url.getProtocol(), url.getAuthority(), path, query, fragment);
		
		String hostname = uri.getHost();
		if (hostname == null) {
			return null;
		}
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
	
	public static void main(String[] args) throws
			URISyntaxException, UnsupportedEncodingException {
		
		System.out.println(getDomainName(URLEncoder.encode(
				"http://www.bbc.co.uk/religion/ethics/torture/" +
						"ethics/wrong_2.shtml|title=Consequentialist","UTF-8")));
	}
}
