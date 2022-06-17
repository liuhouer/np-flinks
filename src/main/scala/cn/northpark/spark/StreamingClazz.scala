package cn.northpark.spark

import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
 * sparkStreaming 转DataFrame
 *
 * 4.从socket获取流式数据，解析映射以后写入到mysql
 *
 * 流式数据结构：
 * 2019-2020,1,3403,家园的治理：环境科学概论,通识课,99.6
 * 2019-2020,1,B0021001,军事理论,通识课,82
 * 2019-2020,2,3509,创业创新领导力,通识课,100
 * 2019-2020,1,B3011023,大学英语BⅠ,通识课,84
 * 2019-2020,1,B2523007,大学生发展规划与就业指导Ⅰ,通识课,89
 * 2019-2020,1,B3211101,思想道德修养与法律基础,通识课,90
 * 2019-2020,2,B3211102,中国近现代史纲要,通识课,90
 * 2019-2020,2,B3011024,大学英语BⅡ,通识课,89
 * 2019-2020,1,B3111017,高等数学A Ⅰ,通识课,83
 * 2019-2020,1,B0041006,校规校纪与安全教育,通识课,95
 * 2019-2020,1,B0024001,社会主义核心价值观培育与践行,通识课,95
 * 2019-2020,1,B3211111,马克思主义中国化进程与青年学生使命担当Ⅰ,通识课,83
 * .
 * .
 * .
 *
 * `tt_clazz`  DDL:
 *
 *   -- spark.tt_clazz definition
 *
 *   CREATE TABLE `tt_clazz` (
 *   `Time` text,
 *   `Term` text,
 *   `ID` text,
 *   `Name` text,
 *   `Type` text,
 *   `Score` double NOT NULL
 *   ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;
 *
 * @author bruce
 * @date 2022年06月17日 09:18:38
 */
object StreamingClazz {

  def main(args: Array[String]): Unit = {


    val spark: SparkSession = SparkSession
      .builder()
      .config("spark.sql.shuffle..partitions",2)
      .master("local[2]")
      .appName("RDD")
      .getOrCreate()

    import spark.implicits._

    val sc: SparkContext = spark.sparkContext

    //开窗口 - 1 min窗口统计
    val ssc: StreamingContext = new StreamingContext(sc,Durations.minutes(1))


    val sqlContext = new SQLContext(sc)


    //通过socket获取实时产生的数据
    val linesRDD = ssc.socketTextStream("node1", 8888)

    /**
     *
     * DS  -> RDD -> DF
     *
     */
    val stuDFs = linesRDD.foreachRDD(rdd => {

      val scoreRdd = rdd.map(line => {
        val split = line.split(",")
        (split(0), split(1), split(2), split(3), split(4), split(5).toDouble)
      })

      val stuDF = scoreRdd.toDF("Time", "Term", "ID", "Name", "Type", "Score")

      stuDF.registerTempTable("tt_clazz")


      //统计平均分并打印
      val tt_clazz = sqlContext.sql("select * from  tt_clazz")
      tt_clazz.show()


      //把统计的平均分写入mysql
      val prop = new Properties()
      prop.put("user","root")
      prop.put("password","123456")
      tt_clazz.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://localhost:3306/spark","tt_clazz",prop)


    })


      //启动任务
      ssc.start()
      //等待任务停止
      ssc.awaitTermination()

    }
  }