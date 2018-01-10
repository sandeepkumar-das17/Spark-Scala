
package com.sandeep.examples;


import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import scala.collection.mutable.Map
import scala.collection.{ Iterator, mutable }
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object firstSpark extends Serializable {

  def main(args: Array[String]) {
    new FirstClass().mainProcessor()
  }

}

class FirstClass {
  
  def mainProcessor():Unit = {
	val spark = SparkSession
  .builder()
  .appName("Spark SQL basic example")
  //.config("spark.some.config.option", "some-value")
  .getOrCreate()

  import spark.implicits._
  import com.datastax.spark.connector._
  import org.apache.spark.sql.Row
  import org.apache.spark.sql.types.{StructType,StructField,StringType}
  
  import org.apache.spark.sql.types._

  // Create an RDD
  val peopleRDD = spark.sparkContext.textFile("src/main/resources/people.txt")
  
  // The schema is encoded in a string
  val schemaString = "name age"
  
  // Generate the schema based on the string of schema
  val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
  val schema = StructType(fields)
  
  // Convert records of the RDD (people) to Rows
  val rowRDD = peopleRDD.map(_.split(",")).map(attributes => Row(attributes(0), attributes(1).trim))
  
  // Apply the schema to the RDD
  val peopleDF = spark.createDataFrame(rowRDD, schema)
  
  // Creates a temporary view using the DataFrame
  peopleDF.createOrReplaceTempView("people")
  
  // SQL can be run over a temporary view created using DataFrames
  val results = spark.sql("SELECT name,age FROM people")
  results.printSchema();
  // The results of SQL queries are DataFrames and support all the normal RDD operations
  // The columns of a row in the result can be accessed by field index or by field name
  results.map(attributes => "Name: " + attributes(0)).show()


	}
}
