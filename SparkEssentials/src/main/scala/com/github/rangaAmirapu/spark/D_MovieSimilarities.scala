package com.github.rangaAmirapu.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import scala.math.sqrt

object D_MovieSimilarities {


  def loadMovieNames() : Map[Int, String] = {
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    var movieNames:Map[Int, String] = Map()
    val lines = Source.fromFile("src/Data/ml-100k/u.item").getLines()
    for(line <- lines){
      var fields = line.split('|')
      if(fields.length > 0){
        movieNames += (fields(0).toInt -> fields(1))
      }
    }
    return movieNames
  }

  type MovieRating = (Int, Double)
  type UserRatingPair = (Int, (MovieRating, MovieRating))

  def filterDuplicates(userRatings:UserRatingPair): Boolean = {
    val movieRating1 = userRatings._2._1
    val movieRating2 = userRatings._2._2

    val movie1 = movieRating1._1
    val movie2 = movieRating2._1

    return movie1 < movie2
  }

  def makePairs(userRatings: UserRatingPair)= {
    val movieRating1 = userRatings._2._1
    val movieRating2 = userRatings._2._2

    val movie1 = movieRating1._1
    val rating1 = movieRating1._2
    val movie2 = movieRating2._1
    val rating2 = movieRating2._2

    ((movie1, movie2), (rating1, rating2))
  }

  type RatingPair = (Double, Double)
  type RatingPairs = Iterable[RatingPair]

  def computeCosineSimilarity(ratingPairs:RatingPairs) : (Double, Int) = {
    var numPairs:Int = 0
    var sum_xx:Double = 0.0
    var sum_yy:Double = 0.0
    var sum_xy:Double = 0.0

    for (pair <- ratingPairs) {
      val ratingX = pair._1
      val ratingY = pair._2

      sum_xx += ratingX * ratingX
      sum_yy += ratingY * ratingY
      sum_xy += ratingX * ratingY
      numPairs += 1
    }

    val numerator:Double = sum_xy
    val denominator = sqrt(sum_xx) * sqrt(sum_yy)

    var score:Double = 0.0
    if (denominator != 0) {
      score = numerator / denominator
    }

    return (score, numPairs)
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]" , "MovieSimilarities")

    println("Loading movie names...")
    val nameDict = loadMovieNames()

    val data = sc.textFile("src/Data/ml-100k/u.item/u.data")

    val ratings = data.map(x => x.split("\t")).map(x => (x(0).toInt, (x(1).toInt, x(2).toDouble)))

    val joinedRatings  = ratings.join(ratings)

    val uniqueJoinedRatings = joinedRatings.filter(filterDuplicates)

    val moviePairs = uniqueJoinedRatings.map(makePairs)

    val moviePairRatings = moviePairs.groupByKey()

    val moviePairSimilarities = moviePairRatings.mapValues(computeCosineSimilarity).cache()

    //val sorted = moviePairSimilarities.sortByKey()
    //sorted.saveAsTextFile("movie-similarities")
    if (args.length > 0) {

      val scoreThreshold = 0.97
      val coOccurenceThreshold = 50.0

      val movieID: Int = args(0).toInt

      val filteredResults = moviePairSimilarities.filter(x => {
        val pair = x._1
        val sim = x._2
        (pair._1 == movieID || pair._2 == movieID) && sim._1 > scoreThreshold && sim._2 > coOccurenceThreshold
      }
      )

      val results = filteredResults.map(x => (x._2, x._1)).sortByKey(false).take(10)

      println("\nTop 10 similar movies for " + nameDict(movieID))

      for (result <- results) {
        val sim = result._1
        val pair = result._2
        // Display the similarity result that isn't the movie we're looking at
        var similarMovieID = pair._1
        if (similarMovieID == movieID) {
          similarMovieID = pair._2
        }
        println(nameDict(similarMovieID) + "\tscore: " + sim._1 + "\tstrength: " + sim._2)
      }
    }
  }

}
