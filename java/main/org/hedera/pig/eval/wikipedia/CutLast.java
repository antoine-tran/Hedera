package org.hedera.pig.eval.wikipedia;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by tuan on 04/06/15.
 */
public class CutLast extends EvalFunc<String> {

    @Override
    public List<FuncSpec> getArgToFuncMapping()
            throws FrontendException {
        List funcList = new ArrayList();
        Schema schema = new Schema();
        schema.add(new Schema.FieldSchema(null,
                DataType.CHARARRAY));
        schema.add(new Schema.FieldSchema(null,
                DataType.INTEGER));
        funcList.add(new FuncSpec(this.getClass().getName(), schema));
        return funcList;
    }

    @Override
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() < 2) {
            return null;
        }
        String s = DataType.toString(input.get(0));
        int size = DataType.toInteger(input.get(1));

        if (s != null) {
            return s.substring(0,s.length()-size);
        }
        return null;
    }
}
