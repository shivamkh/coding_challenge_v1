import java.io._

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.immutable.ListMap
import scala.io.Source

object task1_2 extends App {

  //Creating a Spark session unified entry point for spark application
  val spark = SparkSession.builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local[*]")
    .getOrCreate()

  // the SparkContext is the entry point for low-level APIs, including RDDs
  val sc = spark.sparkContext
  val groceriesRDD = sc.textFile("groceries.csv")

  //Collapsing all the individual rows
  val products = groceriesRDD.flatMap(groceriesRDD => groceriesRDD.split(','))

  //showing first top 50 products in the file
  products.take(50).foreach(println)


  //Creating RDD of all(unique) products
  val uniqueProducts = products.distinct()
  //uniqueProducts.take(200).foreach(println)

  //saving list of all products to the text file
  uniqueProducts.saveAsTextFile("D:\\Spark\\truata_de_coding_challenge_v1\\out\\out_1_2a.txt")

  //saving total product count to text file
  /*
  I have assumed that by total count of product it is meant that how many unique products are
  present.
  If it meant total count of product in all transactions and duplicates are allowed then,
  val count = products.count()
   */
  val count = uniqueProducts.count()
  // FileWriter
  val file = new File("D:\\Spark\\truata_de_coding_challenge_v1\\out\\out_1_2b.txt")
  val bw = new BufferedWriter(new FileWriter(file))
  bw.write(s"Count:\n ${count}")
  bw.close()
}
