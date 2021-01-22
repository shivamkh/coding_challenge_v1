import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.immutable.ListMap
import scala.io.Source

object task1_3 extends App {

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

  //saving output to the text file

  //Calculating individual count for each product
  val productIndividualCount = products.countByValue()

  //sorting products by their count
  val sortedProducts = ListMap(productIndividualCount.toSeq.sortWith(_._2 > _._2): _*)
  //Top 5 purchased products
  val res = sortedProducts.take(5)



  /*val file = new File("D:\\Spark\\truata_de_coding_challenge_v1\\out\\out_1_3.txt")
  val bw = new BufferedWriter(new FileWriter(file))
  res.foreach {
    case (k, v) =>
      bw.write(k + "," + v)
      bw.write("\n")
  }
  bw.close()*/
  //Saving to File
  sc.parallelize(res.toSeq).coalesce(1).saveAsTextFile("D:\\Spark\\truata_de_coding_challenge_v1\\out\\out_1_3.txt")

}
