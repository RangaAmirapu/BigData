package com.github.rangaAmirapu.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._

object F_FindTeenAgers_SQL {

  case class Person(Id:Int, Name:String, Age:Int, NumFriends:Int)

  def mapper(line:String): Person ={
    val fields = line.split(",")
    val person:Person = Person(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt)

    return person
  }


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR);

    val spark = SparkSession
      .builder()
      .appName("F_FindTeenAgers")
      .master("local[*]")
      //.config("spark.sql.warehouse.dir", "file:///C:/temp")
      .getOrCreate()

    val lines = spark.sparkContext.textFile("src/Data/Friends/fakefriends.csv")

    val people = lines.map(mapper)

    import spark.implicits._
    val peopleSchema= people.toDS()

    //peopleSchema.printSchema()
    peopleSchema.createOrReplaceTempView("people")

    val teenAgers = spark.sql("SELECT Age FROM PEOPLE WHERE Age >=13 AND Age <=19")
    val results = teenAgers.collect()
    results.foreach(println)

    spark.stop()

  }

}
