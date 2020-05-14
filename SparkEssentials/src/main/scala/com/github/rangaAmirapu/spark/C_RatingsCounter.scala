package com.github.rangaAmirapu.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object C_RatingsCounter {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "RatingsCounter")

    val lines = sc.textFile("src/Data/ml-100k/u.data")

    val rating = lines.map(x => x.toString().split("\t")(2))

    val results = rating.countByValue();

    val sortedRatings = results.toSeq.sortBy(_._1)

    sortedRatings.foreach(println)

  }

}
