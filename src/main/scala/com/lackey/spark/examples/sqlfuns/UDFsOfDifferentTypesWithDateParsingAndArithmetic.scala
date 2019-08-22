package com.lackey.spark.examples.sqlfuns

import java.io.PrintWriter

import org.apache.spark.sql.{Row, SparkSession}

object UDFsOfDifferentTypesWithDateParsingAndArithmetic extends App {

 def createInputFile: String = {
   import scala.sys.process._

   val outfile = "/tmp/junk.json"
   s"rm -rf $outfile ".!

   val json =
     """{ "user": {"id": "idOfJoe", "name": "joe"},  "dates": ["2016-01-27", "2016-01-28"], "status": "OK", "reason": "something", "content": [{ "foo": 1, "bar": "val1" }, { "foo": 2, "bar": "val2" }, { "foo": 3, "bar": "val3" }, { "foo": 124, "bar": "val4" }, { "foo": 126, "bar": "val5" }] } """
   new PrintWriter(outfile) { write(json); close() }

   outfile
 }

  val sparkSession = SparkSession.builder().appName("simple").master("local[*]").getOrCreate()
  sparkSession.sparkContext.setLogLevel("ERROR")

  import org.apache.spark.sql.functions._
  import sparkSession.implicits._

  // Example of UDF that works on 'user' struct, which consists of a pair of strings.
  // This UDF retuns a new struct consisting of a new string pair, with the position
  // of the two original strings swapped, and some junk added at end of each.
  //
  // The key idea here is that when writing UDFs that take a struct you use type 'Row'
  // to represent the incoming struct.
  //
  val swap: Row => Tuple2[String, String] =
    (struct: Row) => {
      (struct.getAs[String]("id") + "<suffix>", struct.getAs[String]("name") + "<suffix>")
    }
  val swapUdf = udf(swap)


  val df = sparkSession.read.json( createInputFile )
  df.show(false)
  df.printSchema()


  println("Printing diffs between dates")
  df.
    withColumn("numDates", size($"dates")).
    select($"dates"(0).as("date1"), $"dates"($"numDates" - 1).as("date2")).
    select(
      to_date($"date1", "yyyy-MM-dd").as("parsedDate1"),
      to_date($"date2", "yyyy-MM-dd").as("parsedDate2")).
    select(datediff($"parsedDate1",$"parsedDate2").as("daysDifference")).
    show()


  val newDf = df.withColumn("newstruct", swapUdf(df("user")))
  newDf.show(false)
  newDf.printSchema()


  // Example of UDF which takes array of Strings as input, and returns reversed array
  val reverse: Seq[String] => Seq[String] = seq => seq.reverse
  val reverseUdf = udf(reverse)

  val df2 = List((100, Seq[String]("bear", "dog"))).toDF("id", "array")
  df2.printSchema()

  val x = df2.select(reverseUdf($"array"))
  x.show()

  //  result is:
  //  +-----------+
  //  | UDF(array)|
  //  +-----------+
  //  |[dog, bear]|
  //  +-----------+


  // Example of UDF which takes array of Int's as input, and returns reversed array
  //
  // Note that we need to define a type specific version of this UDF since if we tried to
  // do something generic like the below (for this and the previous example)
  //      val reverse2: Seq[Any] => Seq[Any] = seq => seq.reverse
  // we would get the exception: Schema for type Any is not supported
  //
  val df3 = List((100, Seq[Int](3, 4))).toDF("id", "array")

  val reverse2: Seq[Int] => Seq[Int] = seq => seq.reverse
  val reverse2Udf = udf(reverse2)
  val y = df3.select(reverse2Udf($"array"))
  y.show()

  // result is:
  //  +----------+
  //  |UDF(array)|
  //  +----------+
  //  |    [4, 3]|
  //  +----------+



}
