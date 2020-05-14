package com.github.rangaAmirapu.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object C_FriendsByAge {

  def parselines(line: String) = {
    val fields = line.split(",")

    val age = fields(2).toInt

    val numFriends = fields(3).toInt

    (age,numFriends)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "FriendsByAge")

    val lines = sc.textFile("src/Data/Friends/fakefriends.csv")

    val rdd = lines.map(parselines)

    val totalsByAge = rdd.mapValues(x => (x,1)).reduceByKey((x,y) => (x._1 + y._1 , x._2 + y._2))

    val avgByAge = totalsByAge.mapValues(x => x._1 / x._2)

    val results = avgByAge.collect()

    results.sorted.foreach(println)

  }




}
