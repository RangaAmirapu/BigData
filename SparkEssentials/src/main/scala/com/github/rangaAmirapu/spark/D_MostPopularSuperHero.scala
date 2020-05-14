package com.github.rangaAmirapu.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._


object D_MostPopularSuperHero {

  def parseNames(line: String) : Option[(Int, String)] = {
    var fields = line.split('\"')
    if(fields.length > 1){
      return Some(fields(0).trim().toInt, fields(1))
    } else{
      return None
    }
  }

  def countCoOccurences(line:String) ={
    var heroFriends = line.split("\\s+")
    (heroFriends(0).toInt, heroFriends.length - 1)
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local", "MostPopularSuperHero")

    val names = sc.textFile("src/Data/SuperHero/Marvel-names.txt")

    val namesRdd =names.flatMap(parseNames)

    val lines = sc.textFile("src/Data/SuperHero/Marvel-graph.txt")

    val friendsCount = lines.map(countCoOccurences)

    val FriendsCount = friendsCount.reduceByKey((x,y) => x+y).map(x => (x._2, x._1))

    val maxFriendsChar = FriendsCount.max()

    val charName = namesRdd.lookup(maxFriendsChar._2)(0)

    println(s"$charName is the most popular superhero with ${maxFriendsChar._1} co-appearances.")

  }

}
