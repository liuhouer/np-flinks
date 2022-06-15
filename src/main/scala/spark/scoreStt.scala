package spark

import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

/**
 * @author bruce
 * @date 2022年06月15日 16:18:43
 */
object  scoreStt {

  def main(args: Array[String]): Unit = {


    val sparkSession: SparkSession = SparkSession.builder()
      .appName("scoreStt")
      .master("local")
      .getOrCreate()

    val sparkContext: SparkContext = sparkSession.sparkContext

    var path = "C:\\Users\\Bruce\\Desktop\\5\\score.txt"

    val rdd1: RDD[String] = sparkContext.textFile(path)

    // map处理返回bean
    val mapRDD: RDD[Row] = rdd1.map(line => {
      val strings: Array[String] = line.split(",")

      var row: Row = null

      try {
        val stuID: String = strings(0)
        val stuName: String = strings(1)
        val clzID: String = strings(2)
        val clzName: String = strings(3)
        val score: Double = strings(4).toDouble
        val time: String = strings(5)

        // 注意，这里的类型，以及后续的structtype类型需要一一匹配，否则就会出错
        row = Row(stuID, stuName, clzID, clzName,score,time )
      } catch {
        case e:Exception =>{
          e.printStackTrace()
        }
      }

      row
    }).filter(ele=> ele != null)

    // 创建结构化schema信息，注意这里要求是Seq，也就是有序集合，
    // 因为需要按照顺序去解析每个列的字段信息
    val structType: StructType = StructType(List(
      StructField("stuID", DataTypes.StringType),
      StructField("stuName", DataTypes.StringType, false),
      StructField("clzID", DataTypes.StringType, false),
      StructField("clzName", DataTypes.StringType, false),
      StructField("score", DataTypes.DoubleType, false),
      StructField("time", DataTypes.StringType, false)
    ))

    // 通过RDD以及对应的schema信息，创建dataFrame对象
    val dataFrame: DataFrame = sparkSession.createDataFrame(mapRDD, structType)

    // 打印schema信息
    dataFrame.printSchema()

    //注册临时表
    dataFrame.createTempView("tt_score")

    //查询所有数据并打印到控制台
    sparkSession.sql("select * from tt_score").show()

    //统计平均分并打印
    val avgScore = sparkSession.sql("select stuID,stuName,avg(score) from tt_score group by stuID,stuName ")
    avgScore.show()


    //把统计的平均分写入mysql
    val prop = new Properties()
    prop.put("user","root")
    prop.put("password","123456")
    avgScore.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://localhost:3306/spark","tt_score",prop)

    sparkSession.close()

    //======================================================
  }


}
