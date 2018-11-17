package com.cs522

import org.apache.spark._
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.log4j.Level

//import scala.collection.mutable._

object Client extends App {

  case class Pair(period: String, speed: Double)

  case class Pair2(mean: Double, stdDev: Double)

  override def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Spark and SparkSql").setMaster("local")
    val sc = new SparkContext(conf)

    /*
    val csv = sc.textFile("amis.csv")
    val headerAndRows = csv.map(line => line.split(",").map(_.trim))
    val header = headerAndRows.first
    val data = headerAndRows.filter(_(0) != header(0))
    */

    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._


    val csv = sc.textFile("mtcars.csv")
    val headerAndRows = csv.map(line => line.split(",").map(_.trim))
    val header = headerAndRows.first
    val data = headerAndRows.filter(_(0) != header(0))
    val population = data.map(x => Pair(x(2), x(1).toDouble) ).toDF()

    population.printSchema
    population.groupBy("period").agg(mean(population("speed")), stddev(population("speed"))).show()

    var avgMap: Map[String, Pair2] = Map()
    val s = population.sample(false, .25)
    for (i <- 0 until 2) {
      val reSample = s.sample(true, 1)
      val res = reSample.groupBy("period").agg(mean(population("speed")), stddev(population("speed")))
      println("iteration "+i)
      //res.foreach(println)
      res.rdd.foreach(println)

      val result = res.rdd.map(x => (x.get(0).toString, Pair2(x.get(1).toString.toDouble, x.get(2).toString.toDouble) ))
        .reduceByKey((a,b) => {
          val v1 = a.mean + b.mean
          val v2 = a.stdDev + b.stdDev
          println(a.mean + " + " + b.mean)
          Pair2(v1, v2)
        }).collect().toMap
      avgMap = result
      //println(res.rdd)
      //val avg1 = reSample.groupBy("period").mean("speed")
      //val stdDev1 = reSample.groupBy("period")
    }
    println("------")
    println(avgMap.size)
    avgMap.foreach(println)

    val result1 = avgMap.map(x => (x._1, x._2.toString.toDouble * 0.5)).toMap
    result1.foreach(println)
    println("------")


    //println(data)
    //println("Hello World!")
  }
}
