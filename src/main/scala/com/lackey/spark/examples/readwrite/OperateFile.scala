package com.lackey.spark.examples.readwrite

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, SparkSession}

/*
This is a quite common way we process file using sql for now
 */

object OperateFile extends App {
  val sparkSession = SparkSession.builder().appName("simple").master("local[2]").getOrCreate()
  sparkSession.sparkContext.setLogLevel("ERROR")
  val spark = sparkSession
  val filePath = "file:///Users/will/Downloads/spark_homework_will.txt"
  val file = spark.read.format("text").load(filePath)
  val sc = StructType.fromDDL("line STRING")
  val frame = spark.createDataFrame(file.rdd, sc) //change schema
  frame.show()
  frame.createOrReplaceTempView("lines")
  spark.sql("select line, size(split(line, ' ')) as word_cnt from lines").show
}


