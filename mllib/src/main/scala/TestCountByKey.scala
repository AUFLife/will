import java.io.FileWriter

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wzb on 2017/9/5.
  */
object TestCountByKey extends App{

  val conf = new SparkConf()
  conf.setAppName("test parallelize")
    .setMaster("local")
  val sc = new SparkContext(conf)

  // tmaid 48 insideip 15 outsideip 19  host 70
  val count = sc.textFile("D:/Modify/xdr_223/xdr_223/subtract")
    .map(_.split("\\|"))
    .map(x => (x(70), 1))
    .countByKey()

  println(count)
  val out = new FileWriter("D:\\Modify\\xdr_223\\xdr_223\\host")
  count.foreach { line =>
    out.write(line.productIterator.mkString("\t") + "\n")
  }
  out.close()
}
