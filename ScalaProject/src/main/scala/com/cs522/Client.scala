package com.cs522

import java.io._

import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark._
import org.apache.spark.sql.functions._

import scala.collection.mutable

object Client extends App {

  case class Pair(period: String, speed: Double = 0)

  override def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Spark and SparkSql").setMaster("local")
    val sc = new SparkContext(conf)

    val sqlContext = new spark.sql.SparkSession.Builder().config(conf).getOrCreate()
    import sqlContext.implicits._

    val csv = sc.textFile("amis.csv")
    val headerAndRows = csv.map(line => line.split(",").map(_.trim))
    val header = headerAndRows.first
    val data = headerAndRows.filter(_ (0) != header(0))

    val population = data.map(x => Pair(x(2), x(1).toDouble)).toDF()

    population.printSchema
    population.groupBy("period").agg(mean(population("speed")), stddev(population("speed"))).show()

    val numIterations = 100
    val avgMap: mutable.Map[String, (Double, Double)] = mutable.HashMap()
    val s = population.sample(false, .25)
    for (i <- 0 until numIterations) {
      val reSample = s.sample(true, 1)
      val res = reSample.groupBy("period").agg(mean(population("speed")), stddev(population("speed")))
      //println("--iteration " + i)

      val result = res.rdd.map(x => (x.get(0).toString, (x.get(1).toString.toDouble, x.get(2).toString.toDouble)))
        .collect().toMap

      //result.foreach(println)
      avgMap ++= result.map { case (a, b) => {
        val s1 = avgMap.getOrElse(a, (0D, 0D))._1 + (if (b._1.isNaN) 0D else b._1)
        val s2 = avgMap.getOrElse(a, (0D, 0D))._2 + (if (b._2.isNaN) 0D else b._2)
        a -> (s1, s2)
      }
      }
    }
    println("------RESULTS-----")
    val result2 = avgMap.map(x => (x._1, x._2._1/numIterations, x._2._2/numIterations)).toList
    result2.toDF("Period", "Mean", "Variance").show()
  }
}
