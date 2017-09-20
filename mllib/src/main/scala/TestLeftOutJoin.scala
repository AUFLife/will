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
    val rdd2 = sc.parallelize(Map(2 -> "a", 3 -> "b", 4 -> "c").toSeq)

    val rd1 = rdd1.map(x => (x._1, (x._2, 1, 2, 3)))
    val rd2 = rdd2.map(x => (x._1, ((x._2), 3, 2, 1)))

    // LeftOuterJoin， 返回值格式为：(left key, (left value, Option(right value)))
    val res = rd1.leftOuterJoin(rd2)
    res
    res foreach println

  }
}
