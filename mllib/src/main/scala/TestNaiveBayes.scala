import org.apache.log4j.lf5.util.Resource
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession

/**
  * Created by wzb on 2017/9/4.
  */
object TestNaiveBayes extends App{

  val spark = SparkSession
    .builder()
    .master("local[2]")
    .appName("test native bayes")
    .getOrCreate()
  val sc = spark.sparkContext

  val path = "D:\\GitHub\\will\\mllib\\src\\data\\mllib\\sample_naive_bayes_data.txt"
  // Load and parse the data file.
  val data =  sc.textFile(path)
    .map{ line =>
      val value = line.split(" ").toStream.map(_.toDouble).toArray
      // LabeledPint代表一条训练数据，即打过标签的数据
      new LabeledPoint(value(0), Vectors.dense(value))
    }
  val Array(training, test) = data.randomSplit(Array(0.6, 0.4))
  val model = NaiveBayes.train(training, lambda = 1.0, modelType = "multinomial")

  val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))
  val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()

  model.save(sc, "")
//  val sameModel = NaiveBayesModel.load()






}
