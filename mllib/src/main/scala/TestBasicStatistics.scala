import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.sql.SparkSession

/**
  * Created by wzb on 2017/9/1.
  */
object TestBasicStatistics extends App{

  val spark = SparkSession
    .builder()
    .appName("test basic statistics")
    .master("local[2]")
    .getOrCreate()

  val observations = spark.sparkContext.parallelize(
    Seq(
      Vectors.dense(1.0, 1.0, 1.0),
      Vectors.dense(2.0, 2.0, 2.0),
      Vectors.dense(3.0, 3.0, 3.0)
    )
  )
  // compute column summary statistics.
  val summary: MultivariateStatisticalSummary = Statistics.colStats(observations)
  println(summary.mean)       // a dense vector containing the mean value for each column
//  println(summary.variance)   // column-wise variance
//  println(summary.numNonzeros)// number of nonzeros in each columns
}
