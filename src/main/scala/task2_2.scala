import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object task2_2 extends App {

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
    .option("inferSchema", "true") // schema can also be enforced and in production enforcing is better choice
    .option("mode", "permissive") // mode is by default permissive, other possibilities: dropMalformed, failFast
    .parquet("airbnb.parquet")

  //Alternative way as well passing options Map
  val parquetFileDF_alternative = spark.read
    .format("parquet")
    .options(Map("inferSchema" -> "true", // schema can also be enforced and in production enforcing is better choice
      "mode" -> "permissive", // mode is by default permissive, other possibilities: dropMalformed, failFast
      "path" -> "airbnb.parquet"))
    .parquet()

  //parquetFileDF.show()
  //checking schema
  //parquetFileDF.printSchema()

  //Calculating minimum price, maximum price and count
  val task2_2 = parquetFileDF.select(min(col("price").as("min_price")),
    max(col("price").as("max_price")),
    count("*").as("row_count"))

  task2_2.write.format("csv")
    .option("header", "true")
    .save("D:\\Spark\\truata_de_coding_challenge_v1\\out\\out_2_2.csv")

}