package com.lackey.spark.examples.sqlfuns

import java.io.PrintWriter

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

import scala.language.implicitConversions


//  This is a proof of concept.. won't work for all schemas
object SchemaTools extends App  {
  import org.apache.spark.sql.types._

  def noNullSchema(field: StructField): StructField = {
    field.dataType match {
      case  ArrayType(StructType(fields), containsNull) =>
       StructField(
         field.name,
         ArrayType(noNullSchema(StructType(fields)), containsNull),
         nullable = false,
         field.metadata)
      case _ => field.copy(nullable = false)
    }
  }

  def noNullSchema(schema: StructType): StructType =
    StructType (
      schema.fields.map { f =>
        System.out.println("f:" + f);
        noNullSchema(f)
      }
    )

  type Rec = (String, Seq[(Int, String, String)])
  val schema: StructType = Encoders.product[Rec].schema

  System.out.println("pr:" + schema.prettyJson)
  System.out.println("pr:" + noNullSchema(schema).prettyJson)
}


object OfficialAndConvolutedApproachToCountingCorruptRecordsInJsonFile extends App {

  def createInputFile(json: String): String = {
    import java.io.PrintWriter
    import scala.sys.process._

    val outfile = "/tmp/junk.json"
    s"rm -rf $outfile ".!

    new PrintWriter(outfile) {
      write(json); close()
    }
    outfile
  }


  val sparkSession = org.apache.spark.sql.SparkSession.builder().appName("simple").master("local[*]").getOrCreate()
  sparkSession.sparkContext.setLogLevel("ERROR")

  import sparkSession.implicits._

  def countValidAndInvalidJsonRecords(jsonContents: String) = {
    val jFileName = createInputFile(jsonContents)
    val fromFileDS = sparkSession.read.json(jFileName)


    val good = sparkSession.sparkContext.longAccumulator
    val bad = sparkSession.sparkContext.longAccumulator

    if (fromFileDS.columns.contains("_corrupt_record")) {

      val count = fromFileDS.cache().filter($"_corrupt_record".isNotNull).count()
      System.out.println("here is count using 'official' recommended approach:" + count)

      fromFileDS.foreach{ row =>
        if (row.getAs[Any]("_corrupt_record") != null)
          bad.add(1)
        else
          good.add(1)
      }
      (good.count, bad.count)     // This approach is silly.. but seems to work... good accumulator practice !
    } else {
      (fromFileDS.count(), 0L)
    }
  }

  val goodRec = """{ "a": 1, "b": 2 }"""
  val badRec = """!"a": 1, "b": 2 }"""

  assert(countValidAndInvalidJsonRecords(s"$goodRec\n$goodRec") == (2,0))
  assert(countValidAndInvalidJsonRecords(s"$goodRec\n$badRec") == (1,1))

}


object MostlyRecentlyAdded extends App {

  import SchemaTools._


  def createInputFile(json: String): String = {
    import scala.sys.process._

    val outfile = "/tmp/junk.json"
    s"rm -rf $outfile ".!

    new PrintWriter(outfile) {
      write(json); close()
    }
    outfile
  }


  val sparkSession = SparkSession.builder().appName("simple").master("local[*]").getOrCreate()
  sparkSession.sparkContext.setLogLevel("ERROR")

  import sparkSession.implicits._

  // select average score from array of scores
  //
  val avDF = Seq(
    ("joe", Seq(3, 5, 6)),
    ("moe", Seq(8, 2, 2)),
  ).toDF("id", "scoreList")

  avDF.createTempView("scores")

  sparkSession.sql(
    "SELECT (aggregate(scoreList, 0, (acc, x) -> acc + x)) / size(scoreList) as avgScore from scores").show()

  avDF.select(array_contains($"scoreList", 3), array(lit(100), $"scoreList" (1))).show()


  avDF.select(array_remove($"scoreList", 5).as("newArray")).show()

  // Sort an array of structs where the struct initially doesn't have the key in the correct place, but we move it there


  val structDF: Dataset[(String, Seq[(Int, Int, String)])] = Seq(
    ("joe",
      Seq(
        (10, 50, "math"),
        (14, 40, "reading"),
        (12, 30, "history"),
        (19, 20, "gym")
      )
    )
  ).toDS()


  // Try reading identical data from a JSON file while applying a schema to it.
  //
  val json =
  """{"_1":"joe","_2":[{"_1":10,"_2":50,"_3":"math"},{"_1":14,"_2":40,"_3":"reading"},{"_1":12,"_2":30,"_3":"history"},{"_1":19,"_2":20,"_3":"gym"}]}"""
  val jsonBad1 = """{"_2":[{"_1":10,"_2":50,"_3":"math"},{"_1":14,"_2":40,"_3":"reading"},{"_1":12,"_2":30,"_3":"history"},{"_1":19,"_2":20,"_3":"gym"}]}"""
  val jsonBad2 = """{"_2":[{"_1":NOT-INT,"_2":50,"_3":"math"},{"_1":14,"_2":40,"_3":"reading"},{"_1":12,"_2":30,"_3":"history"},{"_1":19,"_2":20,"_3":"gym"}]}"""
  val jsonMaybe =
    """{"_1":"joe","_2":[[10,:50,"math"]]}"""

  val fileContents = Seq(json, jsonBad1, jsonBad2, jsonMaybe ).mkString("\n")


  System.out.println(s"json: $fileContents")
  val jfile = createInputFile(fileContents)

  type Rec = (String, Seq[(Int, Int, String)])
  val schema = Encoders.product[Rec].schema


  // So ... it turns out that even if we set the schema to have fields be non-null, this is advisory only.... and I
  // should have known that, since I asked the question previously:
  // https://stackoverflow.com/questions/56124274/nullability-in-spark-sql-schemas-is-advisory-by-default-what-is-best-way-to-str
  val cleanedSchema = noNullSchema(schema)

  val fromFileDS: Dataset[(String, Seq[(Int, Int, String)])] = sparkSession.read.schema(cleanedSchema).json(jfile).as[Rec]
  println("fromFileDS")
  fromFileDS.show(true)


  // Now sort the array of structs after moving the id to the first element position
  // (which is what sort uses for the order)
  //
  val rearanged = structDF.map { case (name: String, structs: Seq[(Int, Int, String)]) =>
    val idFirstStructs = structs.map { case (best: Int, worst: Int, id: String) =>
      (id, best, worst)
    }

    (name, idFirstStructs)
  }

  //rearanged.show(truncate = false)

  rearanged.select($"_1", array_sort($"_2").as("testResults")).show(truncate = false)

  /// KILL BELOW

  val spark=sparkSession


  import scala.sys.process._

  "rm -rf /tmp/input".!
  "rm -rf /tmp/ff".!
  "rm -rf /tmp/badRecordsPath".!
  "mkdir  /tmp/input".!

  Seq("""{"a": 1, "b": 2}""", """{bad-record""").toDF().write.text("/tmp/input/jsonFile")

  val df = spark.read .option("badRecordsPath", "/tmp/badRecordsPath") .schema("a int, b int").json("/tmp/input/jsonFile")
  df.show()

}
