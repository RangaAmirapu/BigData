package com.github.rangaAmirapu.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.min

/** Find the minimum temperature by weather station */
object C_MinTemp1800 {

  def parseLine(line:String) = {

    //ITE00100554,18000101,TMAX,-75,,,E,

    val fields = line.split(",")
    val stationId = fields(0)
    val entryType = fields(2)
    val temp = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f

    (stationId, entryType, temp)

  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR);

    val sc = new SparkContext("local[*]", "minTemps")

    val plines = sc.textFile("src/Data/Weather/1800.csv")

    val parsedLInes = plines.map(parseLine)

    val minTemps = parsedLInes.filter(x => x._2 == "TMIN")

    val stationTemps = minTemps.map(x => (x._1, x._3.toFloat))

    val minTempByStation = stationTemps.reduceByKey((x,y) => min(x,y))

    val results = minTempByStation.collect()

    for (res <- results){

      val station  = res._1
      val temp = res._2
      val formatedTemp = f"$temp%.2f F"

      println(s"$station min temp: $formatedTemp")

    }

  }
}