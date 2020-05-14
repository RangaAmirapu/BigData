package com.github.rangaAmirapu.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec

object D_MostPopularMovie {

  def loadMovieNames() : Map[Int, String] = {
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    var movieNames: Map[Int, String] = Map()
    val lines = Source.fromFile("src/Data/ml-100k/u.item").getLines()
    for (line <- lines){
      var fields = line.split('|')
      if(fields.length > 1){
        movieNames += (fields(0).toInt -> fields(1))
      }
    }
    return movieNames
  }




  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc  = new SparkContext("local[*]", "MostPopularMovie")

    var nameDict = sc.broadcast(loadMovieNames())

    //196	242	3	881250949
    val movieRatings = sc.textFile("src/Data/ml-100k/u.data")

    val movieIds = movieRatings.map(x => (x.split("\t")(1), 1))

    //(ID, COUNT)
    val movieCountsSorted = movieIds.reduceByKey((x,y) => x+y).collect().sortBy(_._2)

    val movieNameSortedWithNames = movieCountsSorted.map(x => (nameDict.value(x._1.toInt), x._2))

    movieNameSortedWithNames.foreach(println)

  }

}
