package com.github.rangaAmirapu.SparkStreaming

import java.util.concurrent.atomic._

import com.github.rangaAmirapu.SparkStreaming.Utilities._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._

object AvgTweetsLength_Dataset {

  def main(args: Array[String]): Unit = {

    setUpTwitter()

    val ssc = new StreamingContext("local[*]", "AvgTweetsLength_Dataset", Seconds(1))

    setUpLogging()

    val tweets = TwitterUtils.createStream(ssc, None)

    val statuses = tweets.map(status => status.getText())

    val lengths = statuses.map(status => status.length())

    val totalTweets = new AtomicLong(0)
    val totalChars = new AtomicLong(0)
    val longestTweetLength = new AtomicLong(0)

    lengths.foreachRDD((rdd) => {
      var count = rdd.count()
      if(count > 0){
        totalTweets.getAndAdd(count)
        totalChars.getAndAdd(rdd.reduce((x,y) => x+y))

        if(longestTweetLength.get() == 0) {
          longestTweetLength.set(rdd.max())
        }
        else {
          if(longestTweetLength.get() < rdd.max()){
            longestTweetLength.set(rdd.max())
          }
        }

        println("Total Tweets: " + totalTweets.get() +
                " Total chars: " + totalChars.get() +
                " Avg Length: " + totalChars.get()/totalTweets.get() +
                " Longest Length: " + longestTweetLength)
      }
    })

    ssc.checkpoint("D:/SparkCheckPoint/")
    ssc.start();
    ssc.awaitTermination();

  }

}
