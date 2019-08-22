package com.lackey.spark.examples.readwrite

import org.apache.spark.sql.{Row, SparkSession}


object SequenceFileAndJdbc extends App {
  val sparkSession = SparkSession.builder().appName("simple").master("local[*]").getOrCreate()
  sparkSession.sparkContext.setLogLevel("ERROR")
  val spark = sparkSession

  import org.apache.hadoop.io._
  import spark.implicits._

  import sys.process._

  "rm -rf /tmp/null_str".!

  val lines = spark.sparkContext.parallelize(  Seq("fun", "hiho") )
  val withNulls = lines.map{ line => (NullWritable.get, line) }

  withNulls .saveAsSequenceFile("/tmp/null_str")

  val recovered = spark.sparkContext.sequenceFile[NullWritable, String]("/tmp/null_str").map { case (k, v) => v }
  assert(recovered.collect().toList.equals(List("fun", "hiho")))


  // This won't work unless the DB is set up -- I do it like this - first alias is to start server,
  // second is corresponding client connect command
  // alias mysqls="docker run --name some-mysql -e MYSQL_ROOT_PASSWORD=password -p 3306:3306  -d mysql:8.0.17"
  // alias mysqlc="/usr/bin/mysql -h localhost --protocol=tcp  -uroot  -ppassword"
  //
  //mysql> create table foo ( name varchar(256));
  //Query OK, 0 rows affected (0.08 sec)
  //
  //mysql> insert into foo  values ("blah") ;

  //  Best to try this from spark-shell, started up something like this:
  //
  // spark-shell --jars /home/chris/exam/mysql-connector-java-8.0.17.jar

  val props = new java.util.Properties()
  props.setProperty("user","root")
  props.setProperty("password","password")

  val url = "jdbc:mysql://127.0.0.1:3306/mysql"
  val df = spark.read.option("driver", "com.mysql.jdbc.Driver"). jdbc(url, "foo", props)
  assert(df.collect.toList == List(org.apache.spark.sql.Row("blah")))
}


