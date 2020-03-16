package cn.northpark.flink.table_sql_api.stream.sql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

public class Split extends TableFunction<Row> {
    private String separator = ",";
    public Split(String separator) {
        this.separator = separator;
    }
    public void eval(String line){
        for (String s: line.split(separator)){
            collect(Row.of(s));
        }
    }

    @Override
    public TypeInformation getResultType() {
        return Types.ROW(Types.STRING);
    }
}
