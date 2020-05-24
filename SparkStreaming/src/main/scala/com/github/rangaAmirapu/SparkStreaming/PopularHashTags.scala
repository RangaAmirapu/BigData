package com.github.rangaAmirapu.SparkStreaming

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import Utilities._

object PopularHashTags {

  def main(args: Array[String]): Unit = {

    setUpTwitter()

    val ssc = new StreamingContext("local[*]", "PopularHashTags", Seconds(1))

    setUpLogging()

    val tweets = TwitterUtils.createStream(ssc, None)

    val statuses = tweets.map(status => status.getText())

    val tweetWords = statuses.flatMap(tweetText => tweetText.split(" "))

    val hashTags = tweetWords.filter(word => word.startsWith("#"))

    val hashTagKeyValues = hashTags.map(hashTag =>(hashTag, 1))

    val hasTagCounts = hashTagKeyValues.reduceByKeyAndWindow((x,y) => x+y, (x,y) => x-y, Seconds(300), Seconds(1))

    val sortedResult = hasTagCounts.transform(rdd => rdd.sortBy(x => x._2, false));

    sortedResult.print

    ssc.checkpoint("D:\\SparkCheckPoint\\")

    ssc.start()
    ssc.awaitTermination()

  }

}
