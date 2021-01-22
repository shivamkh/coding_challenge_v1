// Import SparkSession and Logisitic Regression
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.IndexToString
import org.apache.spark.sql.SparkSession

//to set the Error reporting
import org.apache.log4j._
Logger.getLogger("org").setLevel(Level.ERROR)

// Spark Session
val spark = SparkSession.builder().config("spark.master", "local").getOrCreate()

// Use Spark to read in the Iris csv file.
val data = spark.read.option("header","true")
  .option("inferSchema","true")
  .format("csv")
  .load("Iris.csv")

//displaying data
val colnames = data.columns
val firstrow = data.head(1)(0)
println("\n")
println("Example Data Row")
for(ind <- Range(1,colnames.length)){
  println(colnames(ind))
  println(firstrow(ind))
  println("\n")
}


// Import VectorAssembler and Vectors
import org.apache.spark.ml.feature.{VectorAssembler,StringIndexer,VectorIndexer,OneHotEncoder}
import org.apache.spark.ml.linalg.Vectors

// Deal with Categorical Columns
val indexer = new StringIndexer()
  .setInputCol("Species")
  .setOutputCol("SpeciesIndex")


val indexed = indexer.fit(data).transform(data)
indexed.show()

//Setting Up DataFrame
import spark.implicits._
val logregdata = indexed.select(indexed("SpeciesIndex").as("label"), $"SepalLengthCm", $"SepalWidthCm",
  $"PetalLengthCm", $"PetalWidthCm")

val assembler = (new VectorAssembler()
  .setInputCols(Array("SepalLengthCm", "SepalWidthCm",
    "PetalLengthCm", "PetalWidthCm")).setOutputCol("features"))

import org.apache.spark.ml.Pipeline

// Creating a new LogisticRegression object called lr
val lr = new LogisticRegression()
  .setRegParam(0.3)

//defining pipeline stages
val pipeline = new Pipeline().setStages(Array(assembler, lr))

//fitting the model as per pipeline
val model = pipeline.fit(logregdata)


/*setting up test data
As, per label encoding

Iris-setosa -> 0.0
Iris-versicolor -> 1.0
Iris-virginica -> 2.0

*/

val test= Seq((5.1, 3.5, 1.4, 0.2),
  (6.2, 3.4, 5.4, 2.3))
  .toDF("SepalLengthCm", "SepalWidthCm", "PetalLengthCm", "PetalWidthCm")


test.show()
val results = model.transform(test)
results.show()

results.select($"prediction".as("class"))
  .write.format("csv")
  .option("header", "true")
  .save("D:\\Spark\\truata_de_coding_challenge_v1\\out\\out_3_2b.csv")


