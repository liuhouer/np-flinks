package cn.northpark.flink.scala.suicideApp

import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.api.bridge.scala.BatchTableEnvironment
import org.apache.flink.table.descriptors.{FileSystem, OldCsv, Schema}
import org.apache.flink.types.Row
// 导入scala的隐式转换
import org.apache.flink.api.scala.{ExecutionEnvironment, _}


/**
 * ！！！hadoop hdfs连接不上 直接把9000端口去掉就可以了！！！！
 * @author bruce
 * @date 2022年04月18日 10:48:03
 *
 *
 *       序号	字段名	数据类型	字段描述
 *       1	country	Integer	城市
 *       2	year	String	年份
 *       3	sex	String	性别
 *       4	age	String	年龄
 *       5	suicides_no	String	自杀率
 *       6	population	String	人口
 *       7	suicides/100k pop	String	每100K人
 *       8	country-year	String	城市-年份
 *       9	HDI for year	String	人类发展指数
 *       10	gdp_for_year ($)	String	年GDP
 *       11	gdp_per_capita ($)	String	人均GDP
 */
object  suicideApp {

  // 自定义类型
//  case class Bean(
//                   country: String,
//                   years: String,
//                   sex: String,
//                   age: String,
//                   suicides_no: int,
//                   population: String,
//                   suicides_100k_pop: double,
//                   country_year: String,
//                   hdi_for_year: String,
//                   gdp_for_year: int,
//                   gdp_per_capita: int
//
//                 )

  def main(args: Array[String]): Unit = {

    //todo -修改- 配置输出路径
    val BASE_OUT_DIR = "C:\\Users\\Bruce\\Desktop\\3\\"

    // 初始化执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    val tableEnv = BatchTableEnvironment.create(env)


    val connectorDescriptor = new FileSystem().path("hdfs://node1/scd/master.csv")

    val tableSchema = new TableSchema.Builder()
      .field("country", Types.STRING)
      .field("years", Types.STRING)
      .field("sex", Types.STRING)
      .field("age", Types.STRING)
      .field("suicides_no", Types.INT)
      .field("population", Types.STRING)
      .field("suicides_100k_pop", Types.DOUBLE)
      .field("country_year", Types.STRING)
      .field("hdi_for_year", Types.STRING)
      .field("gdp_for_year", Types.INT)
      .field("gdp_per_capita", Types.INT)
      .build()

    val format = new OldCsv()
      .schema(tableSchema)
      .ignoreFirstLine()
      .ignoreParseErrors()
      .fieldDelimiter(",")
      .lineDelimiter("\n")


    tableEnv.connect(connectorDescriptor)
      .withSchema(new Schema().schema(tableSchema))
      .withFormat(format)
      .createTemporaryTable("scd")

    //print all columns
//    val all_metrics = tableEnv.sqlQuery("select * from scd  ")
//    all_metrics.printSchema()
//    tableEnv.toDataSet[Row](all_metrics).print()


    //sex metrics
    val sex_metrics = tableEnv.sqlQuery("select sex,age,avg(suicides_no) as nums from scd group by sex,age order by nums desc ")
    tableEnv.toDataSet[Row](sex_metrics).print()
    tableEnv.toDataSet[(String,String, Int)](sex_metrics).setParallelism(1).writeAsCsv(BASE_OUT_DIR+"sex_metrics.csv","\n",",", org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE)

    //age metrics
    val age_metrics = tableEnv.sqlQuery("select age,avg(suicides_no) as nums from scd group by age order by nums desc ")
    tableEnv.toDataSet[Row](age_metrics).print()
    tableEnv.toDataSet[(String, Int)](age_metrics).setParallelism(1).writeAsCsv(BASE_OUT_DIR+"age_metrics.csv","\n",",", org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE)


    //gdp year  metrics
    val gdp_year_metrics = tableEnv.sqlQuery("select years,avg(gdp_for_year) as gdp,avg(suicides_no) as nums from scd group by years order by nums desc ")
    tableEnv.toDataSet[Row](gdp_year_metrics).print()
    tableEnv.toDataSet[(String,Int, Int)](gdp_year_metrics).setParallelism(1).writeAsCsv(BASE_OUT_DIR+"gdp_year_metrics.csv","\n",",", org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE)


    //gdp per   metrics
    val gdp_per_metrics = tableEnv.sqlQuery("select years,avg(gdp_per_capita) as gdp,avg(suicides_no) as nums from scd group by years order by nums desc ")
    tableEnv.toDataSet[Row](gdp_per_metrics).print()
    tableEnv.toDataSet[(String,Int, Int)](gdp_per_metrics).setParallelism(1).writeAsCsv(BASE_OUT_DIR+"gdp_per_metrics.csv","\n",",", org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE)


    //population metrics
    val population_metrics = tableEnv.sqlQuery("select population,avg(suicides_100k_pop) as nums from scd group by population  order by nums desc ")
    tableEnv.toDataSet[Row](population_metrics).print()
    tableEnv.toDataSet[(String, Double)](population_metrics).setParallelism(1).writeAsCsv(BASE_OUT_DIR+"population_metrics.csv","\n",",", org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE)


    //HDI metrics
    val hdi_metrics = tableEnv.sqlQuery(" select hdi_for_year ,nums from (select hdi_for_year,avg(suicides_no) as nums from scd  group by hdi_for_year ) where hdi_for_year IS NOT NULL  order by nums desc  ")
    tableEnv.toDataSet[(String, Int)](hdi_metrics).print()
    tableEnv.toDataSet[(String, Int)](hdi_metrics).setParallelism(1).writeAsCsv(BASE_OUT_DIR+"hdi_metrics.csv","\n",",", org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE)


    env.execute("suicideApp")

  }
}
