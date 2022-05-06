package cn.northpark.flink.scala.suicideApp

import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.table.api.bridge.scala.BatchTableEnvironment
import org.apache.flink.types.Row

// 导入scala的隐式转换
import org.apache.flink.api.scala.{ExecutionEnvironment, _}

/**
 *
 * ！！！hadoop hdfs连接不上 直接把9000端口去掉就可以了！！！！
  * 使用 batch env的api 读取数据，返回DataSet对象
  * 使用 table batch env将dataset注册为table，使用 registerDataSet方法
  * 通过tableEnv.sqlQuery进行sql查询
  */
object ReadTableCsvFile {

  def main(args: Array[String]): Unit = {

    //todo -修改- 配置输出路径
    val BASE_OUT_DIR = "c://Users/Bruce/Desktop/3/"

    val env = ExecutionEnvironment.getExecutionEnvironment

    val tableEnv = BatchTableEnvironment.create(env)


    val inputHDFS: DataSet[Bean] = env.readCsvFile[Bean]("C:\\Users\\Bruce\\Desktop\\np\\master.csv", ignoreFirstLine = true,lineDelimiter="\n",fieldDelimiter = ",")


    inputHDFS.print();



    // 将DataSet转成Table对象
    val table = tableEnv.fromDataSet(inputHDFS)

    // 注册 Table
    tableEnv.registerTable("scd", table)

    //sex metrics
    val sex_metrics = tableEnv.sqlQuery("select sex,age,avg(CAST(suicides_no AS Int)) as nums from scd group by sex,age order by nums desc ")
    tableEnv.toDataSet[Row](sex_metrics).print()
    tableEnv.toDataSet[(String,String, Int)](sex_metrics).setParallelism(1).writeAsCsv(BASE_OUT_DIR+"sex_metrics.csv", "\n",",",WriteMode.OVERWRITE)

    //age metrics
    val age_metrics = tableEnv.sqlQuery("select age,avg(CAST(suicides_no AS Int)) as nums from scd group by age order by nums desc ")
    tableEnv.toDataSet[Row](age_metrics).print()
    tableEnv.toDataSet[(String, Int)](age_metrics).setParallelism(1).writeAsCsv(BASE_OUT_DIR+"age_metrics.csv","\n",",", org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE)


    //gdp year  metrics
    val gdp_year_metrics = tableEnv.sqlQuery("select years,avg(CAST(gdp_for_year AS Int)) as gdp,avg(CAST(suicides_no AS Int)) as nums from scd group by years order by nums desc ")
    tableEnv.toDataSet[Row](gdp_year_metrics).print()
    tableEnv.toDataSet[(String,Int, Int)](gdp_year_metrics).setParallelism(1).writeAsCsv(BASE_OUT_DIR+"gdp_year_metrics.csv","\n",",", org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE)


    //gdp per   metrics
    val gdp_per_metrics = tableEnv.sqlQuery("select years,avg(CAST(gdp_per_capita AS Double)) as gdp,avg(CAST(suicides_no AS Int)) as nums from scd group by years order by nums desc ")
    tableEnv.toDataSet[Row](gdp_per_metrics).print()
    tableEnv.toDataSet[(String,Double, Int)](gdp_per_metrics).setParallelism(1).writeAsCsv(BASE_OUT_DIR+"gdp_per_metrics.csv","\n",",", org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE)


    //population metrics
    val population_metrics = tableEnv.sqlQuery("select population,avg(CAST(suicides_100k_pop AS Double)) as nums from scd group by population  order by nums desc limit 10")
    tableEnv.toDataSet[Row](population_metrics).print()
    tableEnv.toDataSet[(String, Double)](population_metrics).setParallelism(1).writeAsCsv(BASE_OUT_DIR+"population_metrics.csv","\n",",", org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE)


    //HDI metrics
    val hdi_metrics = tableEnv.sqlQuery(" select hdi_for_year ,nums from (select hdi_for_year,avg(CAST(suicides_no AS Int)) as nums from scd  group by hdi_for_year ) where hdi_for_year IS NOT NULL  order by nums desc  limit 10")
    tableEnv.toDataSet[(String, Int)](hdi_metrics).print()
    tableEnv.toDataSet[(String, Int)](hdi_metrics).setParallelism(1).writeAsCsv(BASE_OUT_DIR+"hdi_metrics.csv","\n",",", org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE)


    env.execute("ReadTableCsvFile3")
  }



  case class Bean(
                   country: String,
                   years: String,
                   sex: String,
                   age: String,
                   suicides_no: String,
                   population: String,
                   suicides_100k_pop: String,
                   country_year: String,
                   hdi_for_year: String,
                   gdp_for_year: String,
                   gdp_per_capita: String
                 )
}
