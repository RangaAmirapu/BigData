package com.github.rangaAmirapu.SparkStreaming

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import org.apache.log4j.Level
import Utilities._

object PrintTweets {

  def main(args: Array[String]): Unit = {

    setUpTwitter()

    val ssc = new StreamingContext("local[*]", "PrintTweets", Seconds(1))

    setUpLogging()

    val tweets = TwitterUtils.createStream(ssc, None)

    val statuses = tweets.map(status => status.getText())

    statuses.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
