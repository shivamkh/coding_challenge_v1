import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object task2_4 extends App {

  val spark = SparkSession.builder()
    .appName("Reading Parquet File")
    .config("spark.master", "local")
    .getOrCreate()

  /*
  Reading DataFrames require
  - format
  - schema or inferSchema = true
  - path
  - zero or more options
  */
  val parquetFileDF = spark.read
    .option("inferSchema", "true")    // schema can also be enforced and in production enforcing is better choice
    .option("mode", "permissive")     // mode is by default permissive, other possibilities: dropMalformed, failFast
    .parquet("airbnb.parquet")

  //Alternative way as well passing options Map
  val parquetFileDF_alternative = spark.read
    .format("parquet")
    .options(Map("inferSchema"->"true", // schema can also be enforced and in production enforcing is better choice
      "mode"->"permissive",              // mode is by default permissive, other possibilities: dropMalformed, failFast
      "path"->"airbnb.parquet"))
    .parquet()

  //parquetFileDF.show()
  //checking schema
  //parquetFileDF.printSchema()

  //Calculating minimum price
  val min_price = parquetFileDF.select(min(col("price")).as("min_price")).collect


  //val maximum rating
  val max_rating = parquetFileDF.select(max(col("review_scores_rating")).as("rating")).collect

  //Calculating accommodating capacity for maximum rated cheapest property
  parquetFileDF.filter(col("price")===min_price(0)(0) and col("review_scores_rating")===max_rating(0)(0))
    .select(col("accommodates")).write.format("csv")
    .option("header", "true")
    .save("D:\\Spark\\truata_de_coding_challenge_v1\\out\\out_2_4.csv")


}
