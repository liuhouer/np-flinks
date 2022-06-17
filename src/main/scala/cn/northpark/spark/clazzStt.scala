package cn.northpark.spark

import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

/**
 * @author bruce
 * @date 2022年06月15日 16:18:43
 *      3. 统计科目的平均分
 *
 *
 *       txt数据格式：
 *       <pre>
 *       Time,Term,ID,Name,Type,Score
 *       2019-2020,1,3403,家园的治理：环境科学概论,通识课,99.6
 *       2019-2020,1,B0021001,军事理论,通识课,82
 *       2019-2020,2,3509,创业创新领导力,通识课,100
 *       2019-2020,1,B3011023,大学英语BⅠ,通识课,84
 *       2019-2020,1,B2523007,大学生发展规划与就业指导Ⅰ,通识课,89
 *       2019-2020,1,B3211101,思想道德修养与法律基础,通识课,90
 *       2019-2020,2,B3211102,中国近现代史纲要,通识课,90
 *       2019-2020,2,B3011024,大学英语BⅡ,通识课,89
 *       2019-2020,1,B3111017,高等数学A Ⅰ,通识课,83
 *       2019-2020,1,B0041006,校规校纪与安全教育,通识课,95
 *       2019-2020,1,B0024001,社会主义核心价值观培育与践行,通识课,95
 *       2019-2020,1,B3211111,马克思主义中国化进程与青年学生使命担当Ⅰ,通识课,83
 *       2019-2020,1,B0041007,军事训练,通识课,90
 *       2019-2020,1,B2011078,大数据导论,通识课,100
 *       2019-2020,1,B2021009,C语言程序设计基础,专业课,81
 *       2019-2020,2,B2011001,离散数学,专业课,100
 *       2019-2020,2,B2021001,数据结构,专业课,95
 *       2019-2020,2,B2021002,计算机组成原理,专业课,94
 *       2019-2020,2,B0021002,大学生心理健康教育,通识课,83
 *       2019-2020,2,B2051001,高级语言课程设计,专业课,90
 *       2019-2020,2,B3111007,高等数学AⅡ,通识课,94
 *       2019-2020,2,ty0001,篮球,通识课,80
 *       2019-2020,1,ty0016,网球,通识课,85
 *       2020-2021,2,3681,会计学原理,通识课,93
 *       2020-2021,1,B3211103,马克思主义基本原理概论,通识课,78
 *       2020-2021,2,B3211104,毛泽东思想和中国特色社会主义理论体系概论,通识课,84
 *       2020-2021,1,B2523008,大学生发展规划与就业指导Ⅱ,通识课,84
 *       2020-2021,2,B2523010,创业基础与实践Ⅰ,通识课,78
 *       2020-2021,1,B2523014,创新思维训练,通识课,100
 *       2020-2021,2,B3211105,形势与政策Ⅰ,通识课,90
 *       2020-2021,1,B3211107,形势与政策Ⅲ,通识课,80
 *       2020-2021,2,B3111116,大学物理导论,通识课,83
 *       2020-2021,2,B3211108,形势与政策Ⅳ,通识课,90
 *       2020-2021,1,B3011025,大学英语BⅢ,通识课,86
 *       2020-2021,2,B3011026,大学英语BⅣ,通识课,88
 *       2020-2021,1,B3211112,马克思主义中国化进程与青年学生使命担当Ⅱ,通识课,88
 *       2020-2021,1,B2021008,Java面向对象程序设计,专业课,73
 *       2020-2021,1,B2021003,操作系统,专业课,82
 *       2020-2021,1,B2011003,形式语言与自动机,专业课,90
 *       2020-2021,1,B2021007,Python程序设计,专业课,86
 *       2020-2021,1,B2023002,开源大数据核心技术,专业课,89
 *       2020-2021,1,B2023004,Web前端技术,专业课,95
 *       2020-2021,1,B2051002,数据结构与算法课程设计,专业课,85
 *       2020-2021,1,B2051003,Linux操作系统,专业课,92
 *       2020-2021,2,B2011002,软件工程,专业课,85
 *       2020-2021,2,B2021004,数据库概论,专业课,94
 *       2020-2021,2,B2011005,阿里云大咖课堂,专业课,90
 *       2020-2021,2,B2021006,机器学习基础,专业课,77
 *       2020-2021,2,B2013002,企业大数据技术与应用,专业课,100
 *       2020-2021,2,B2023003,Python数据分析技术,专业课,100
 *       2020-2021,2,B2051004,MySQL数据库技术,专业课,72
 *       2020-2021,1,B3111010,线性代数A,通识课,85
 *       2020-2021,1,B3111013,概率论与数理统计B,通识课,83
 *       2020-2021,2,B3131104,大学物理实验B,通识课,90
 *       2020-2021,2,ty0001,篮球,通识课,81
 *       2020-2021,1,ty0022,武术,通识课,88
 *       2021-2022,1,3348,传统文化与现代经营管理,通识课,98
 *       2021-2022,1,3680,法律基础,通识课,98
 *       2021-2022,1,B2523011,创业基础与实践Ⅱ,通识课,90
 *       2021-2022,1,B3211106,形势与政策Ⅱ,通识课,86
 *       2021-2022,1,B3211109,形势与政策Ⅴ,通识课,92
 *       2021-2022,1,B2523024,大学生发展规划与就业指导Ⅲ,通识课,88
 *       2021-2022,1,B2023005,Java EE应用开发技术,专业课,90
 *       2021-2022,1,B2023008,数据仓库理论与实践,专业课,92
 *       2021-2022,1,B2021005,计算机网络,专业课,79
 *       2021-2022,1,B2011006,云计算与大数据,专业课,87
 *       2021-2022,1,B2023006,Java海量数据分布式开发,专业课,93
 *       2021-2022,1,B2051005,大数据推荐系统实践,专业课,92
 *
 *       </pre>
 *
 *
 *       数据表`tt_clazz_avg`  DDL:
 *
 *       -- spark.tt_clazz_avg definition
 *
 *       CREATE TABLE `tt_clazz_avg` (
 *       `Type` text NOT NULL,
 *       `score` double DEFAULT NULL
 *       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;
 *
 *
 *
 *
 */
object clazzStt {

  def main(args: Array[String]): Unit = {


    val sparkSession: SparkSession = SparkSession.builder()
      .appName("scoreStt")
      .master("local")
      .getOrCreate()

    val sparkContext: SparkContext = sparkSession.sparkContext

    var path = "C:\\Users\\Bruce\\Desktop\\Score.txt"

    val rdd1: RDD[String] = sparkContext.textFile(path)

    println(rdd1);

    // map处理返回bean
    val mapRDD: RDD[Row] = rdd1
      //跳过标题首行
      .mapPartitionsWithIndex((idx, iter) => if (idx == 0) iter.drop(1) else iter)
      .map(line => {
      val strings: Array[String] = line.split(",")
        strings.foreach((itt)=>println("strings###",itt))


      //Time	Term	ID	Name	Type	Score
      var row: Row = null

      try {
        val Time: String = strings(0)
        val Term: String = strings(1)
        val ID: String = strings(2)
        val Name: String = strings(3)
        val Type: String = strings(4)
        val Score: Double = strings(5).toDouble

        // 注意，这里的类型，以及后续的structtype类型需要一一匹配，否则就会出错
        row = Row(Time, Term, ID, Name,Type,Score )
      } catch {
        case e:Exception =>{
          e.printStackTrace()
        }
      }

      row
    }).filter(ele=>ele!=null)
    //.filter(ele=> ele.getString(5).equals("专业课"))

    // 创建结构化schema信息，注意这里要求是Seq，也就是有序集合，
    // 因为需要按照顺序去解析每个列的字段信息
    val structType: StructType = StructType(List(
      StructField("Time", DataTypes.StringType),
      StructField("Term", DataTypes.StringType, false),
      StructField("ID", DataTypes.StringType, false),
      StructField("Name", DataTypes.StringType, false),
      StructField("Type", DataTypes.StringType, false),
      StructField("Score", DataTypes.DoubleType, false)
    ))

    // 通过RDD以及对应的schema信息，创建dataFrame对象
    val dataFrame: DataFrame = sparkSession.createDataFrame(mapRDD, structType)

    // 打印schema信息
    dataFrame.printSchema()

    //注册临时表
    dataFrame.createTempView("tt_clazz")

    //查询所有数据并打印到控制台
    sparkSession.sql("select * from tt_clazz").show()
//
    //统计平均分并打印
    val avgScore = sparkSession.sql("select Type,avg(score) as score from tt_clazz group by Type ")
    avgScore.show()


    //把统计的平均分写入mysql
    val prop = new Properties()
    prop.put("user","root")
    prop.put("password","123456")
    avgScore.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://localhost:3306/spark","tt_clazz_avg",prop)

    sparkSession.close()

    //======================================================
  }


}
