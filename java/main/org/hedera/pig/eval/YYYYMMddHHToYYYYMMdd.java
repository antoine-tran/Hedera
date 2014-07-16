package org.hedera.pig.eval;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/** Use Jodatime to convert string of format 'YYYYMMdd' to 'YYYYMMddHH' */
public class YYYYMMddHHToYYYYMMdd  extends EvalFunc<String> {
	
	private static final DateTimeFormatter OUTPUT_TIME_FORMAT 
	= DateTimeFormat.forPattern("YYYYMMdd");
	
	private static final DateTimeFormatter INPUT_TIME_FORMAT 
	= DateTimeFormat.forPattern("YYYYMMddHH");

	@Override
	public String exec(Tuple input) throws IOException {

		if (input == null || input.size() < 1) {
			return null;
		}

		// Set the time to default or the output is in UTC
		DateTimeZone.setDefault(DateTimeZone.UTC);

		DateTime dt = INPUT_TIME_FORMAT.parseDateTime(DataType.toString(input.get(0)));
		String res = OUTPUT_TIME_FORMAT.print(dt);
		return res;		
	}

	@Override
	public Schema outputSchema(Schema input) {

		return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), 
				DataType.CHARARRAY));

	}



	@Override

	public List getArgToFuncMapping() throws FrontendException {

		List funcList = new ArrayList();

		funcList.add(new FuncSpec(this.getClass().getName(), new Schema(new Schema.FieldSchema(null, DataType.LONG))));



		return funcList;

	}
}
