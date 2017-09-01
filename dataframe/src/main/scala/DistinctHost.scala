import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by wzb on 2017/8/17.
  */
case class People(host: String, url: String)

object DistinctHost {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("distinct")
      .master("local[2]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    import spark.implicits._

    val df = spark.sparkContext.textFile("D:\\Modify\\beijing/item")
      .map(_.split("\\|"))
//    val df = spark.read.textFile("D:\\Modify\\beijing/test2")

    val schemaString = "host\turl"
    val fields = schemaString.split("\t")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    // Convert record of the RDD(people) to Rows
    val rowRDD = df.map{ attr =>
      Row(attr(0), attr(1).trim)
    }

    // Apply the schema to the RDD
    val DF = spark.createDataFrame(rowRDD, schema)
    // Create a temporary view using the DataFrame
    DF.createOrReplaceTempView("xdr")

    // SQL can be run over a temporary view created using DataFrames
    val results = spark.sql("select distinct host from xdr")

    results.map(attr => "Name: " + attr(0)).show()

  }
}
