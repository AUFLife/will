import org.apache.spark.rdd.RDD

/**
  * Created by wzb on 2017/8/31.
  */
object TestNaiveBayes extends App{


  val SEPARATOR1 = "\t"
  val SEPARATOR2 = "_"


  def NBmodels(line:String, cidMap:Map[String,Int]): String = {
    val record = line.split(SEPARATOR1)
    val manNum = cidMap.get("CA_男").getOrElse(0).toDouble
    val womanNum = cidMap.get("CA_女").getOrElse(0).toDouble
    val sum = cidMap.get("SUM").getOrElse(0).toDouble
    // 计算先验概率，这里采取了拉普拉斯平滑处理，解决冷启动问题
    val manRate = (manNum + 1)/(sum + 2)
    val womanRate = (womanNum + 1)/(sum + 2)
    var manProbability = 1.0
    val womanProbabilty = 1.0
    for (i <- 1 until record.length) {
      // 组合key键
      val womanKey = "CF" + i + "_" + record(i) + "_" + "女“"
      val manKey = "CF" + i + "_" + record(i) + "_" + "男"
      val catWoman = "CA" + SEPARATOR2 + "女"
      val catMan = "CA" + SEPARATOR2 + "男"
      // 确定特征向量空间的种类，解决冷启动问题
      val num = 3
      // 获取训练模型得到的结果
//      val manValue = div()
    }
  }

  /**
    *
    * @param rdd  训练样本RDD，训练样本输出路径
    * @param path
    */
  def NBmodelformat(rdd:RDD[String], path: String) = {
    val allCompute = rdd.map(_.split(SEPARATOR1)).map(record =>
      // SEPARATOR0定义分隔符，这里为"\u0009"
      {
        var str = ""
        val lengthParam = record.length
        for (i <- 1 until lengthParam) {
          if (i < lengthParam - 1) {
            val standKey = "CF" + i + SEPARATOR2 + record(i) + SEPARATOR2 + record(lengthParam - 1)
            // 对特征与类别的关联值进行计数
            str = str.concat(standKey).concat(SEPARATOR1)
          } else {
            // 对分类（男/女）进行计数
            val standKey = "CA" + SEPARATOR2 + record(lengthParam - 1)
            str = str.concat(standKey).concat(SEPARATOR1)
          }
        }
        // 对样本总数进行计数
        str.concat("SUM").trim
      }
    ).flatMap(_.split(SEPARATOR1)).map((_, 1)).reduceByKey(_+_)
    // 本地输出一个文件，保存到本地目录
    allCompute.repartition(1).saveAsTextFile(path)
  }
}
