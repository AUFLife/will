import org.apache.spark.sql.SparkSession

/**
  * Created by wzb on 2017/9/20.
  */
object TestLeftOutJoin {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("test leftOutJoin")
      .getOrCreate()
    val sc = spark.sparkContext
    val rdd1 = sc.parallelize(Map(1 -> 1, 2 -> 2, 3 -> 3).toSeq)
    val rdd2 = sc.parallelize(Map(2 -> 2, 3 -> 3, 4 -> 4).toSeq)

    val res = rdd1.leftOuterJoin(rdd2)
    res foreach println

  }
}
