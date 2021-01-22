import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.immutable.ListMap
import scala.io.Source

object task1_1 extends App {

  //Creating a Spark session unified entry point for spark application
  val spark = SparkSession.builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local[*]")
    .getOrCreate()

  // the SparkContext is the entry point for low-level APIs, including RDDs
  val sc = spark.sparkContext
  val groceriesRDD = sc.textFile("groceries.csv")


  //showing top 5 rows
  groceriesRDD.take(5).foreach(println)
}