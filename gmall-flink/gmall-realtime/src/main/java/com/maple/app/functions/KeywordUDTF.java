package com.maple.app.functions;

import com.maple.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.IOException;

@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeywordUDTF extends TableFunction<Row> {
    public void eval(String text) throws IOException {
        for (String keyword : KeywordUtil.jiebaSplit(text)) {
            collect(Row.of(keyword));
        }
    }
}