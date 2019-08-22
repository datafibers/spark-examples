package com.lackey.spark.examples.readwrite

import java.io.File

import org.apache.spark.sql.{Dataset, Row, SparkSession}


object PartitionsBucketsDDLFormat extends App {
  val sparkSession = SparkSession.builder().appName("simple").master("local[2]").getOrCreate()
  sparkSession.sparkContext.setLogLevel("ERROR")
  val spark = sparkSession

  import spark.implicits._

  // Partitioning demo
  //
  case class Person(first: String, last: String, age: Integer)

  val df = List(
    Person("moe", "x", 2),
    Person("mob", "x", 2),
    Person("mob", "x", 2),
    Person("zike", "y", 2),
    Person("zike", "y", 3),
    Person("red", "y", 2),
    Person("red", "z", 2)).toDF()


  // Overwrite existing and verify partition directories are as expected
  // (for the four names: moe,mob,zike,red)
  //
  df.write.partitionBy("first").mode("overwrite").csv("/tmp/output.mouse")
  assert(new File("/tmp/output.mouse").listFiles().count(f => f.getName.startsWith("first=")) == 4)




  // Bucketing demo
  //
  spark.sql("drop table if exists cogbox").show()
  spark.sql("show tables").show()     // this shows the table is gone, but... seems to linger when run from IDE

  // So,  we need to delete the cogbox directory. If we don't we can't recreate the table. This doesn't happen in
  // spark shell.. not sure why ...  punting for now
  import scala.sys.process._
  "rm -rf spark-warehouse/cogbox".!

  import org.apache.spark.sql.Row
  case class Parson(first: String, last: String, age: Integer)
  val pdf = List(
    Parson("moe", "x", 4),
    Parson("jox", "x", 3),
    Parson("pop", "x", 1),
    Parson("bob", "y", 2)).toDF()

  // BAD:
  // for me, this gives: AnalysisException: 'save' does not support bucketBy right now
  // pdf.write.option("mode","overwrite").bucketBy(3, "last", "age").parquet("/tmp/rrcogbox")

  // This works
  pdf.write.option("mode", "overwrite").bucketBy(2, "first").saveAsTable("cogbox")

  val d2 = spark.sql("select * from cogbox")
  val first: Row = d2.orderBy($"first").first
  System.out.println("first:" + first);
  assert(first == Row("bob", "y", 2))
  d2.show()
  assert(spark.sql("select distinct first from cogbox").collect().toList.size == 4)

  val pdf3 = List(Parson("axel", "x", 9)).toDF()
  pdf3.write.insertInto("cogbox")
  spark.sql("select distinct first from cogbox").show()

  assert(spark.sql("select distinct first from cogbox").collect().toList.size == 5)  // one more than before




  // DDL formatted schema - you need a schema if you want to convert an RDD of Row to DataFrame
  //
  import org.apache.spark.sql.types._

  val rdd = spark.sparkContext.parallelize(List(Row("joe", 9)))


  // The statement below won't work, 'cause you need a schema when dealing with rows
  // spark.createDataFrame( rdd)

  val sc = StructType.fromDDL("name STRING, rank INT")
  val frame = spark.createDataFrame(rdd, sc).select($"rank" + 1)
  frame.show()
  assert(frame.collect().toList.head.getAs[Integer](0) == 10)

  val sc2 = StructType(List(StructField("name", StringType), StructField("rank", IntegerType)))
  assert(sc2 == sc)


  // RDD's of product are directly convertible to datasets
  case class Foo(x: Integer)
  val x: Dataset[Foo] = spark.sparkContext.parallelize(List(Foo(2))).toDS()

}


