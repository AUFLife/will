import java.util

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StringType, StructField, StructType}

/**
  * Created by wzb on 2017/8/23.
  */
/**
  * Creating Datasets
  * Datasets instead of using Java seriallization or Kryo they use a speciallized Encoder to seriallzie the objects for
  * processing or transmitting over the network.While both encoders nad standard seriallization are reponsible for turning an object into bytes,
  * encoders are code generated dynamically and use a format that allows SPark to perform many operations like filtering, sorting and hashing without deseriallzing the bytes back into an object.
  */
case class Xdr(host: String, url: String)

object TestDataFrame {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    // Inferring the Schema Using Reflection
//    val xdrDF = spark.sparkContext.textFile("D:\\Modify\\beijing/test2")
//      .map(_.split("\t"))
//      .map(attr => Xdr(attr(0), attr(1)))
//      .toDF()
//    // Register the DataFrame as a Temporary view
//    xdrDF.createOrReplaceTempView("xdr")
//
//
//    val resDF = spark.sql(("select host, url from xdr"))
//    resDF.map(x => x(0) + "\t").show()
//
//    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
//    resDF.map(df => df.getValuesMap[Any](List("host", "url"))).collect()


    // Programmatically Specifying the Schema
    val schemaString = "host\turl"
    val xdrRDD = spark.sparkContext.textFile("D:\\Modify\\beijing/test2")
    // Generate the schema based on the string of schema
    val fields = schemaString.split("\t")
      .map(field => DataTypes.createStructField(field, DataTypes.StringType, true))

    val schema = DataTypes.createStructType(fields)


    //    val fields = schemaString.split("\t").map(fieldName => StructField(fieldName, StringType, nullable = true))
//    val schema = StructType(field)

    // Convert record of the RDD (xdr) to Rows
    val rowRDD= xdrRDD
      .map(_.split("\t"))
      .map(attr => Row(attr(0), attr(1)))
    // apply the schema to the RDD
    val xdrDF = spark.createDataFrame(rowRDD, schema)

    // Creates a temporary view using the DataFrame
    xdrDF.createOrReplaceTempView("xdr")

    // SQL can be run over a temporary view created using DataFrames
    val results: DataFrame = spark.sql("select distinct host from xdr")

    results.rdd.saveAsTextFile("D:\\Modify/test")

//    results.write.json("D:\\Modify/test")

    results.map(attr => "Host: " + attr(0)).show()
  }
}
