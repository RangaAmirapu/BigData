package com.github.rangaAmirapu.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object C_WordCountInBook {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "wordCountInBook")

    val input = sc.textFile("src/Data/Book/book.txt")

    val words = input.flatMap(x => x.split("\\W+"))

    val lowerCaseWords = words.map(x => x.toLowerCase())

    val wordCounts = lowerCaseWords.map(x => (x,1)).reduceByKey((x,y) => x+y)

    val wordCountSorted = wordCounts.map(x => (x._2, x._1)).sortByKey().collect()

    for (result <- wordCountSorted){
      val count = result._1
      val word = result._2
      println(s"$word : $count")
    }

  }

}
