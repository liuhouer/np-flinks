package cn.northpark.flink;

import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * @author bruce
 * @date 2023年03月15日 17:39:12
 * mysql 关联 csv的数据集，更新mysql表的部分字段数据
 *
 */
public class MysqlConnCsvUpdateAttractions {

    public static void main(String[] args) throws Exception{
        // Set up the Flink batch environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Set up the JDBC input format for the attractions table
        String attractionsQuery = "SELECT name, region, category, address, comment, price, sales, province, longitude, latitude FROM t_attractions";
        JDBCInputFormat inputFormat = JDBCInputFormat.buildJDBCInputFormat()
                .setDrivername("com.mysql.jdbc.Driver")
                .setDBUrl("jdbc:mysql://localhost:3306/mydb")
                .setUsername("myuser")
                .setPassword("mypassword")
                .setQuery(attractionsQuery)
                .setRowTypeInfo(new RowTypeInfo(
                        Types.STRING, // name
                        Types.STRING, // region
                        Types.STRING, // category
                        Types.STRING, // address
                        Types.STRING, // comment
                        Types.DOUBLE, // price
                        Types.INT,    // sales
                        Types.STRING, // province
                        Types.DOUBLE, // longitude
                        Types.DOUBLE  // latitude
                ))
                .finish();

        // Read the attractions table as a DataSet<Row>
        DataSet<Row> attractions = env.createInput(inputFormat);

        // Set up the CSV file format for the Excel file
        String filePath = "/path/to/excel/file.xlsx";
        Csv csv = new Csv()
                .fieldDelimiter('\t')
//                .ignoreFirstLine(true)
                .ignoreParseErrors()
                .allowComments()
                .schema(TableSchema.builder()
                        .field("name", DataTypes.STRING())
                        .field("region", DataTypes.STRING())
                        .field("category", DataTypes.STRING())
                        .field("longitude", DataTypes.DOUBLE())
                        .field("latitude", DataTypes.DOUBLE())
                        .build());

        // Set up the Flink table environment
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        // Register the attractions table as a Flink table
        tEnv.createTemporaryView("t_attractions", attractions, "name, region, category, address, comment, price, sales, province, longitude, latitude");

        // Register the Excel file as a Flink table
        tEnv.connect(new FileSystem().path(filePath))
                .withFormat(csv)
                .withSchema(new Schema()
                        .field("name", DataTypes.STRING())
                        .field("region", DataTypes.STRING())
                        .field("category", DataTypes.STRING())
                        .field("longitude", DataTypes.DOUBLE())
                        .field("latitude", DataTypes.DOUBLE())
                )
                .createTemporaryTable("t_excel");

// Define a scalar function to update the attractions table
        tEnv.registerFunction("updateAttractions", new UpdateAttractions());

        // Join the attractions table with the Excel table
        Table result = tEnv.sqlQuery(
                "SELECT " +
                        "a.name, a.region, a.category, a.address, a.comment, a.price, a.sales, a.province, " +
                        "IF(e.longitude IS NULL, a.longitude, e.longitude) AS longitude, " +
                        "IF(e.latitude IS NULL, a.latitude, e.latitude) AS latitude " +
                        "FROM t_attractions a " +
                        "LEFT JOIN t_excel e ON a.name = e.name"
        );

        // Convert the result to a DataSet<Row>
        DataSet<Row> output = tEnv.toDataSet(result, Row.class);

        // Set up the JDBC output format for the attractions table
        JDBCOutputFormat outputFormat = JDBCOutputFormat.buildJDBCOutputFormat()
                .setDrivername("com.mysql.jdbc.Driver")
                .setDBUrl("jdbc:mysql://localhost:3306/mydb")
                .setUsername("myuser")
                .setPassword("mypassword")
                .setQuery("UPDATE t_attractions SET longitude=?, latitude=? WHERE name=?")
                .setSqlTypes(new int[] { Types.DOUBLE, Types.DOUBLE, Types.STRING })
                .finish();

// Write the output to the attractions table
        output.output(outputFormat);

// Execute the Flink job
        env.execute("UpdateAttractions");
    }


    static class UpdateAttractions extends ScalarFunction {
        public void eval(
                String name,
                String region,
                String category,
                String address,
                String comment,
                double price,
                int sales,
                String province,
                double longitude,
                double latitude,
                double newLongitude,
                double newLatitude,
                Collector<Row> out
        ) {
            if (newLongitude != 0 && newLatitude != 0) {
                out.collect(Row.of(name, region, category, address, comment, price, sales, province, newLongitude, newLatitude));
            } else {
                out.collect(Row.of(name, region, category, address, comment, price, sales, province, longitude, latitude));
            }
        }
    }

}
