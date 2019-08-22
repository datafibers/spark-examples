package com.lackey.spark.examples.stream

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{explode, split}

object StructuredStreamingOverSocket extends App {
  val spark = SparkSession.builder().master("local[*]").appName("bad-dog").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val lines = spark.readStream.format("socket").option("host", "localhost").option("port", "9999").load()

  val grouped = lines.select(explode(split($"value", " ")).as("word")).groupBy($"word").count()
  val query   = grouped.writeStream.format("console").outputMode("complete").start()

  //val words = lines.as[String].flatMap(_.split(" "))
  //val wordCounts = words.groupBy("value").count()
  //val query = wordCounts.writeStream .outputMode("complete") .format("console") .start()


  query.awaitTermination()
}
