package cn.northpark.flink.scala.transformApp.util

import org.apache.commons.math3.random.{GaussianRandomGenerator, JDKRandomGenerator}

object GaussTest {
  def main(args: Array[String]): Unit = {
    val generator = new GaussianRandomGenerator(new JDKRandomGenerator())
    for(i <- 1 to 100){
      val result: Int =  (generator.nextNormalizedDouble() * 100).abs.toInt
      println(result)
    }
  }

}
