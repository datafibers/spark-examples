package com.lackey.spark.examples.dataset

import org.apache.calcite.avatica.ColumnMetaData
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrameReader, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


// See: https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-functions-collection.html
//
object CollectionFunctions extends App {

  val conf =
    new SparkConf().setAppName("blah").
      setMaster("local").set("spark.sql.shuffle.partitions", "2")
  val sparkSession = SparkSession.builder.config(conf).getOrCreate()
  val spark = sparkSession

  import spark.implicits._

  sparkSession.sparkContext.setLogLevel("ERROR")


  import org.apache.spark.sql.types._
  val json =
    """{"name":"n1","email":"n1@c1.com","company":{"cName":"c1","cId":"1","details":"d1"}}
      |{"name":"n2","id":"2","email":"n2@c1.com","company":{"cName":"c1","cId":"1","details":"d1"}}
      |{"name":"n3","id":"3","email":"n3@c1.com","company":{"cName":"c1","cId":"1","details":"d1"}}
      |{"name":"n4","id":"4","email":"n4@c2.com","company":{"cName":"c2","cId":"2","details":"d2"}}
      |{"name":"n5","email":"n5@c2.com","company":{"cName":"c2","cId":"2","details":"d2"}}
      |{"name":"n6","id":"6","email":"n6@c2.com","company":{"cName":"c2","cId":"2","details":"d2"}}
      |{"name":"n7","id":"7","email":"n7@c3.com","company":{"cName":"c3","cId":"3","details":"d3"}}
      |{"name":"n8","id":"8","email":"n8@c3.com","company":{"cName":"c3","cId":"3","details":"d3"}}
      |""".stripMargin('|')



  System.out.println("json:" + json);

  val schema2 = StructType(
    List(
      StructField("name", StringType),
      StructField("email", StringType),
      StructField("id", StringType),
      StructField("company",
        StructType(
          List(
            StructField("cName", StringType),
            StructField("cId", StringType),
            StructField("details", StringType)
          )
        )
      )
    )
  )

  val frame = List(json).toDF("lines").select(explode(split($"lines", "\\n")).as("line"))
  frame.printSchema()
  frame.show(truncate = false)

  val contents = frame.select(from_json($"line", schema2).as("structo"))
  val expanded = contents.select($"structo.*", $"structo.company.*").drop("company").na.fill("0", Seq("id"))
  expanded.show(false)              // This shows that we have one row with some nulls

  val cleaned = expanded.na.drop().withColumn("id", $"id".cast(IntegerType))
  // so we drop the row w/ the null columns and display the result
  cleaned .show()
  // RESULT:
  //+----+---------+---+-----+---+-------+
  //|name|    email| id|cName|cId|details|
  //+----+---------+---+-----+---+-------+
  //|  n1|n1@c1.com|  0|   c1|  1|     d1|
  //|  n2|n2@c1.com|  2|   c1|  1|     d1|
  //|  n3|n3@c1.com|  3|   c1|  1|     d1|
  //|  n4|n4@c2.com|  4|   c2|  2|     d2|
  //|  n5|n5@c2.com|  0|   c2|  2|     d2|
  //|  n6|n6@c2.com|  6|   c2|  2|     d2|
  //|  n7|n7@c3.com|  7|   c3|  3|     d3|
  //|  n8|n8@c3.com|  8|   c3|  3|     d3|
  //+----+---------+---+-----+---+-------+

  // Group by cName and show total of id column (as int) for each group
  val grouped = cleaned.groupBy($"cName").agg(sum($"id"))
  grouped.show()
  // RESULT:
  //+-----+--------------------+
  //|cName|sum(CAST(id AS INT))|
  //+-----+--------------------+
  //|   c1|                   5|
  //|   c3|                  15|
  //|   c2|                  10|
  //+-----+--------------------+


  // Show the dense rank of each user, where rank is determined by highest id
  import org.apache.spark.sql.expressions.Window
  val ranked = cleaned.withColumn("rank", dense_rank().over(Window.orderBy($"id".desc)))
  ranked.show()
  //RESULT
  //+----+---------+---+-----+---+-------+----+
  //|name|    email| id|cName|cId|details|rank|
  //+----+---------+---+-----+---+-------+----+
  //|  n8|n8@c3.com|  8|   c3|  3|     d3|   1|
  //|  n7|n7@c3.com|  7|   c3|  3|     d3|   2|
  //|  n6|n6@c2.com|  6|   c2|  2|     d2|   3|
  //|  n4|n4@c2.com|  4|   c2|  2|     d2|   4|
  //|  n3|n3@c1.com|  3|   c1|  1|     d1|   5|
  //|  n2|n2@c1.com|  2|   c1|  1|     d1|   6|
  //|  n1|n1@c1.com|  0|   c1|  1|     d1|   7|
  //|  n5|n5@c2.com|  0|   c2|  2|     d2|   7|
  //+----+---------+---+-----+---+-------+----+


  val singersDF = Seq(
    ("beatles", "help|hey jude|help|help2"),
    ("romeo", "eres mia")
  ).toDF("name", "hit_songs")

  val actualDF = singersDF.withColumn(
    "hit_songs",
    split(col("hit_songs"), "\\|")
  )

  actualDF.show(truncate=false)
  actualDF.printSchema()

  val contains =
    actualDF.select(
      array_contains($"hit_songs", "help"),
      array_contains($"hit_songs", "not-in-list"))

  contains.show()   // should be true, false for first rec, false,false for next one after that

  Seq(Array(0,1,2)).toDF("array").withColumn("num", explode('array)).show
  // RESULT
  //+---------+---+
  //|    array|num|
  //+---------+---+
  //|[0, 1, 2]|  0|
  //|[0, 1, 2]|  1|
  //|[0, 1, 2]|  2|
  //+---------+---+



  // from_json
  //
  val jsons = Seq("""{ "id": 0 }""").toDF("json")

  import org.apache.spark.sql.types._

  val schema0 = new StructType()
    .add($"id".int.copy(nullable = false))

  import org.apache.spark.sql.functions.from_json

  jsons.select(from_json($"json", schema0) as "ids").show
  // RESULT
  //+---+
  //|ids|
  //+---+
  //|[0]|
  //+---+

  import org.apache.spark.sql.types._

  val addressesSchema = new StructType()
    .add($"city".string)
    .add($"state".string)
    .add($"zip".string)
  val schema = new StructType()
    .add($"firstName".string)
    .add($"lastName".string)
    .add($"email".string)
    .add($"addresses".array(addressesSchema))

  // Generate the JSON-encoded schema
  // That's the variant of the schema that from_json accepts
  val schemaAsJson = schema.json

  val rawJsons = Seq(
    """
    {
      "firstName" : "Jacek",
      "lastName" : "Laskowski",
      "email" : "jacek@japila.pl",
      "addresses" : [
        {
          "city" : "Warsaw",
          "state" : "N/A",
          "zip" : "02-791"
        },
        {
          "city" : "boston",
          "state" : "N/A",
          "zip" : "22"
        }
      ]
    }
  """).toDF("rawjson")
  val people = rawJsons
    .select(from_json($"rawjson", schemaAsJson, Map.empty[String, String]) as "json")
    .select("json.*") // <-- flatten the struct field
    .withColumn("address", explode($"addresses")) // <-- explode the array field
    .drop("addresses") // <-- no longer needed
    .select("firstName", "lastName", "email", "address.*") // <-- flatten the struct field
  // RESULT
  //+---------+---------+---------------+------+-----+------+
  //|firstName| lastName|          email|  city|state|   zip|
  //+---------+---------+---------------+------+-----+------+
  //|    Jacek|Laskowski|jacek@japila.pl|Warsaw|  N/A|02-791|
  //+---------+---------+---------------+------+-----+------+


  // Array contains
  import org.apache.spark.sql.functions.array_contains

  val c = array_contains(column = $"ids", value = 1)

  val ids = Seq(Seq(1, 2, 3), Seq(1), Seq(2, 3)).toDF("ids")
  val q = ids.filter(c)
  q.show
  // RESULT
  //+---------+
  //|      ids|
  //+---------+
  //|[1, 2, 3]|
  //|      [1]|
  //+---------+



  // to json function

  import org.apache.spark.sql._
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types._

  // Convenience function for turning JSON strings into DataFrames.
  def jsonToDataFrame(json: String, schema: StructType = null): DataFrame = {
    // SparkSessions are available with Spark 2.0+
    val reader: DataFrameReader = spark.read
    Option(schema).foreach(reader.schema)
    val jj = spark.sparkContext.parallelize(Array(json))
    System.out.println("jj:" + jj.collect().toList);
    reader.json(jj)
  }


  val events = jsonToDataFrame("""
  {
    "a": {
      "b": 1
    }
  }
  """)


  events.select(to_json($"a").as("json")).show()
}




