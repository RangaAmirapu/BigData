package com.github.rangaAmirapu.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._

object F_FindTeenAgers_DataFrames {

  case class Person(Id:Int, Name:String, Age:Int, NumFriends:Int)

  def mapper(line:String):Person ={
    val fields = line.split(",")
    val person:Person = Person(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt)
    return person
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("F_FindTeenAgers_DataFrames")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val lines = spark.sparkContext.textFile("src/Data/Friends/fakefriends.csv")
    val people = lines.map(mapper).toDS().cache()

    println("Schema is")
    people.printSchema()

    println("All Names")
    people.select("Name").show()

    println("People less than 21")
    people.filter(people("Age") < 21).show()

    println("Group by age")
    people.groupBy("Age").count().sort("Age").show()

    println("Make everyone ten years old")
    people.select(people("Name"), people("Age") + 10).sort("Age").show()

    spark.stop()

  }

}
