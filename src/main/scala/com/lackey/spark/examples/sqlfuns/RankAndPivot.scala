package com.lackey.spark.examples.sqlfuns

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window

object RankAndPivot extends App {


  val sparkSession = SparkSession.builder().appName("simple").master("local[*]").getOrCreate()
  sparkSession.sparkContext.setLogLevel("ERROR")

  import org.apache.spark.sql.functions._
  import sparkSession.implicits._

  case class Race(raceName: String, runnerName: String, time: Int)

  val rdd = sparkSession.sparkContext.parallelize(
    Seq(
      Race("mile", "a", 2), // first
      Race("mile", "b", 5),
      Race("mile", "c", 5),
      Race("mile", "d", 9),
      Race("sprint", "x", 2),
      Race("sprint", "y", 9),
      Race("sprint", "z", 8),
      Race("sprint", "p", 3) // last
    )
  )

  val df = rdd.toDF()
  df.show()

  df.select( first($"runnerName") ).show()    // show one result row with 'a'
  df.select( last($"runnerName") ).show()     // show one result row with 'p'

  val window = Window.partitionBy($"raceName").orderBy($"time")
  val ranked = df.withColumn("rank", dense_rank.over(window))
  ranked.show()
  //
  //RESULT:
  //+--------+----------+----+----+
  //|raceName|runnerName|time|rank|
  //+--------+----------+----+----+
  //|  sprint|         x|   2|   1|
  //|  sprint|         p|   3|   2|
  //|  sprint|         z|   8|   3|
  //|  sprint|         y|   9|   4|
  //|    mile|         a|   2|   1|
  //|    mile|         b|   5|   2|
  //|    mile|         c|   5|   2|
  //|    mile|         d|   9|   3|
  //+--------+----------+----+----+



  val rdd2 = sparkSession.sparkContext.parallelize(
    Seq(
      Race("mile", "a", 2),
      Race("mile", "a", 5),
      Race("mile", "a", 5),
      Race("mile", "d", 9),
      Race("mile", "d", 5),
      Race("mile", "x", 2),
      Race("mile", "x", 4),
      Race("sprint", "x", 2),
      Race("sprint", "y", 9),
      Race("sprint", "z", 8),
      Race("sprint", "p", 3),
      Race("walk", "a", 3),
      Race("walk", "a", 9),
      Race("walk", "p", 3)
    )
  )

   val df2 = rdd2.toDF()
   val piv = df2.groupBy($"runnerName").pivot("raceName").agg(avg($"time"))
   piv.show()
  // RESULT
  //+----------+----+------+----+
  //|runnerName|mile|sprint|walk|
  //+----------+----+------+----+
  //|         x| 3.0|   2.0|null|
  //|         z|null|   8.0|null|
  //|         p|null|   3.0| 3.0|
  //|         d| 7.0|  null|null|
  //|         y|null|   9.0|null|
  //|         a| 4.0|  null| 6.0|
  //+----------+----+------+----+


  // Now lets get the nulls out, we will represent a null number as the string '-', but
  // the na function will not work unless we cast the number typed columns  to string
  var stringDF = piv
  piv.columns.foreach{c =>  stringDF = stringDF.withColumn(c, col(c).cast("string")) }
  stringDF .na.fill("-").show()


}
