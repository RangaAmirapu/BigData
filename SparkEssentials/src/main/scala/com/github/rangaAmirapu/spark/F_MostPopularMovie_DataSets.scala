package com.github.rangaAmirapu.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import org.apache.spark.sql.functions._

object F_MostPopularMovie_DataSets {

  def loadMovieNames(): Map[Int, String] = {
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    var movieNames:Map[Int, String] = Map()
    val lines = Source.fromFile("src/Data/ml-100k/u.item").getLines()

    for(line <- lines){
      var fields = line.split('|')
      if(fields.length > 1){
        movieNames += (fields(0).toInt -> fields(1))
      }
    }
    return movieNames
  }


  final case class  Movie(movieId: Int)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder.appName("F_MostPopularMovie_DataSets").master("local[*]").getOrCreate()

    import spark.implicits._
    val movieDS = spark.sparkContext.textFile("src/Data/ml-100k/u.data")
      .map(x => Movie(x.split("\t")(1).toInt)).toDS()

    val topMovieIDS = movieDS.groupBy("movieId").count().orderBy(desc("count")).cache()

    val top10Movies = topMovieIDS.take(10)

    val names = loadMovieNames()

    for(m <- top10Movies){
      println(names(m(0).asInstanceOf[Int]) + ":" + m(1))
    }

    spark.stop()

  }

}
