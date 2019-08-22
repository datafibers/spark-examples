package com.lackey.spark.examples.stream

import java.io.{File, FileOutputStream, PrintWriter}

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types._

import scala.collection.{immutable, mutable}
import scala.concurrent.{Await, Future}

//  This demo illustrates how input that fits in a certain time window 'W' is held from being output until a record
//  comes in with a time stamp 't' such that t + watermark > W.end
//  Demo can be run in mode where we force the pending window's results with the closing record
//  joe|0|1970-02-02|dummy"      (pass forceLastWindow = false), or in a mode where we don't have that
//  closing record, in which case we will never see the final window.
//
object StructuredStreamingOutputToJsonFileDemo {
  def main(args: Array[String]): Unit = {

    import TestFixtures._

    def runTest(forceLastWindow : Boolean): Unit = {
      val spark: SparkSession = SparkSession.builder()
        .master("local[3]")
        .appName("SparkByExample")
        .getOrCreate()
      spark.sparkContext.setLogLevel("ERROR")

      val timezone = "PST"

      init(forceLastWindow)
      val schema = spark.read.option("delimiter", "|").option("header", "true").csv(inputStaged).schema
      schema.printTreeString()

      moveInputFromStagingToIncoming()

      val csvDf =
        spark.readStream.
          option("delimiter", "|").option("header", "true").schema(schema).csv(incomingJsonFilesDir)

      import spark.implicits._

      val grouped =
        csvDf.select($"name", $"score".cast(IntegerType), $"date".cast(TimestampType))
          .withWatermark("date", "1 day")
          .groupBy(window($"date", "10 day", "10 day").as("date_window"), $"name")
          .agg(sum($"score"))
          .withColumn("windowEnd", date_format(to_utc_timestamp($"date_window.end", timezone), "yyyy-MM-dd"))
          .withColumn("windowStart", date_format(to_utc_timestamp($"date_window.start", timezone), "yyyy-MM-dd"))
          .drop($"date_window")

      val query: StreamingQuery = grouped.coalesce(1).writeStream
        .format("json")
        .option("path", jsonOutputDir)
        .option("checkpointLocation", checkpointDir)
        .outputMode("append")
        .start()

      import scala.concurrent.{Await, Future}
      import scala.concurrent.duration._
      val f: Future[Unit] = launchSleepAndCheckResultsThread(spark, forceLastWindow, query)
      Await.result(f, 60 second)
    }

    runTest(false)
    runTest(true)
  }
}

object TestFixtures extends App {

  import scala.sys.process._

  def incomingJsonFilesDir  = "/tmp/incomingJsonFilesDir"
  def checkpointDir  = "/tmp/checkpoint"
  def inputStagingDir = "/tmp/inputStagingDir"
  def jsonOutputDir = "/tmp/jsonOutputDir"
  def inputFileName = "input.csv"
  def inputStaged = s"$inputStagingDir/$inputFileName"
  def inputIncoming = s"$incomingJsonFilesDir/$inputFileName"

  def init(forceLastWindow : Boolean  = false): Unit = {
    List(incomingJsonFilesDir, checkpointDir, inputStagingDir, jsonOutputDir ).foreach { dir =>
      s"rm -rf $dir".!
      s"mkdir $dir".!
    }

    val input =
      """name|score|date|comment
        |joe|2|1970-01-01|should fall in 10 window [ 1970-01-01 , 1970-01-11 ]
        |joe|3|1970-01-11|should fall in 10 window [ 1970-01-11 , 1970-01-21 ]
        |joe|4|1970-01-22|should fall in 10 window [ 1970-01-21 , 1970-01-31 ]
        |joe|6|1970-01-24|should fall in 10 window [ 1970-01-21 , 1970-01-31 ]""".stripMargin
    val dummy = "\njoe|0|1970-02-02|dummy"
    val finalInput = if (forceLastWindow)  input + dummy  else input
    new PrintWriter(new FileOutputStream(s"$inputStaged")) { write(finalInput) ; close() }

    ()    // void return
  }

  def moveInputFromStagingToIncoming() = s"mv $inputStaged $inputIncoming".!

  import scala.concurrent.ExecutionContext.Implicits._

  def launchSleepAndCheckResultsThread(spark: SparkSession, forceLastWindow : Boolean, query: StreamingQuery)  = Future {
    try {
      val expectedRows = Set(
        Row("joe", 2,  "1970-01-01","1970-01-11"),
        Row("joe", 3,   "1970-01-11", "1970-01-21")
      )

      val expectedIfForced = Row("joe", 10,"1970-01-21", "1970-01-31")

      println("running loop to block until we have good output ")
      var results: Set[Row] = null
      while(results == null) {
        try {   // keep trying to read the output, then set the results once we succeed
          import spark.implicits._
          val df = spark.read.json(jsonOutputDir).select($"name", $"sum(score)", $"windowStart", $"windowEnd")
          results = df.collect().toSet
        } catch { case e: Throwable =>  ()  }
      }

      val expected = if (forceLastWindow)  expectedRows + expectedIfForced else expectedRows
      if (results  != expected )
        println(s"BAD: $results")
      else
        println(s"GOOD: $results")
      query.stop()
    } catch { case e: Throwable => e.printStackTrace() }
  }
}

