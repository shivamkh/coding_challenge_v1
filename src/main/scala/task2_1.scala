import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object task2_1 extends App {

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

  parquetFileDF.show()
  parquetFileDF.printSchema()

}