package hr.fer.ztel.thesis.spark

import org.apache.spark.sql.SparkSession

class SparkSessionHandler(args : Array[String]) extends Serializable {

  val folderPah : String = if (!args(0).endsWith("/")) args(0) + "/" else args(0)
  val customerItemPath : String = folderPah + args(1)
  val itemItemPath : String = folderPah + args(2)
  val recommendationsPath : String = folderPah + args(3) + "_" + System.currentTimeMillis
  val topK : Int = args(4).toInt

  val customersSizePath : String = customerItemPath + ".lookup.size"
  val itemsSizePath : String = itemItemPath + ".lookup.size"

  def getSparkSession : SparkSession = {

    val sparkBuilder =
      if (folderPah.startsWith("hdfs:///")) SparkSession.builder
      else SparkSession.builder.master("local[*]")

    sparkBuilder
      .appName("SparkRecommenderThesis")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", "hr.fer.ztel.thesis.spark.SparkKryoRegistrator")
      .getOrCreate
  }

}

object SparkSessionHandler extends Serializable {
  val argsMessage = "Wrong args, should be: [folder] [customer-item] [item-item] [recommendations] [k]"
}