package com.github.rangaAmirapu.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object C_AmountSpentByCustomer {

  def extractCustomerPricePairs(line: String) ={

    val fields = line.split(",")
    (fields(0).toInt, fields(2).toFloat)

  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "AmountOrderedByCustomer")

    val input = sc.textFile("src/Data/CustOrders/customer-orders.csv")

    val mappedInput = input.map(extractCustomerPricePairs)

    val amountByCust = mappedInput.reduceByKey((x,y) => x + y)

    val amountByCustFlipped = amountByCust.collect().sortBy(_._2)

    amountByCustFlipped.foreach(println)

  }
}