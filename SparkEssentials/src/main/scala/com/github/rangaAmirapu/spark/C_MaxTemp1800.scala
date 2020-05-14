package com.github.rangaAmirapu.spark

import org.apache.spark.{SPARK_BRANCH, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.log4j._

import scala.math.min

object C_MaxTemp1800 {

  def parseLine(line:String) = {

    //ITE00100554,18000101,TMAX,-75,,,E,
    val fields = line.split(",")
    val stationId = fields(0)
    val entityType = fields(2)
    val temp = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f

    (stationId, entityType, temp)

  }

  def main(args: Array[String]): Unit ={

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "1800MaxTemps")

    val lines = sc.textFile("src/Data/Weather/1800.csv")

    val parsedLines = lines.map(parseLine)

    val maxTemps = parsedLines.filter(x => x._2 == "TMAX")

    val stationTemps = maxTemps.map(x => (x._1, x._3.toFloat))

    val maxTempByStation = stationTemps.reduceByKey((x,y) => min(x,y))

    val result = maxTempByStation.collect()

    for (res <- result){
      val station = res._1
      val temp = res._2
      println(s"$station max temp: $temp")
    }


  }

}
