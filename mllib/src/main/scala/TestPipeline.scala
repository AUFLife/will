import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * Created by wzb on 2017/8/30.
  */
object TestPipeline extends App{

  val spark = SparkSession
    .builder()
    .master("local[4]")
    .appName("testPipeline")
    .getOrCreate()

  // Prepare traing data.
  val training = spark.createDataFrame(Seq(
    (1.0, Vectors.dense(0.0, 1.1, 0.1)),
    (0.0, Vectors.dense(2.0, 1.0, -1.0)),
    (0.0, Vectors.dense(2.0, 1.3, 1.0)),
    (1.0, Vectors.dense(0.0, 1.2, -0.5))
  )).toDF("label", "features")

  // Create a LogisticRegression instance. This instance is an Estimator
  val lr = new LogisticRegression()
  // Print out the parameters, documentation and any default values
  println("LogisticRegression parameters:\n" + lr.explainParams() + "\n")

  // We may set parameters using setter methods.
  lr.setMaxIter(10)
    .setRegParam(0.01)

  // Learn a LogisticRegression model. This uses the parameters stored in lr.
  val model1 = lr.fit(training)
  // Since model1 is a Model(i.e., a Transformer produced by an Estimator),
  // we can view the parameters it used during fit()
  // This prints the parameter(name: value)pairs, where names are unique IDs for this
  // LogisticRegression instance.
  println("Model 1 was fit using parameters: " + model1)

  // we may alternatively specify parameters using a ParamMap,
  // which supports several methods for specifying parameters.



}
