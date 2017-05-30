package hr.fer.ztel.thesis.spark

import org.apache.spark.sql.SparkSession

class SparkSessionHandler(args : Array[String]) extends Serializable {

  if (args.length != 6) {
    println("Wrong args, should be: [folder] [customer-item] [item-item] [normalize] [recommendations] [k] ")
    System exit 1
  }

  val folder : String = if (!args(0).endsWith("/")) args(0) + "/" else args(0)
  val customerItemPath : String = folder + args(1)
  val itemItemPath : String = folder + args(2)
  val normalize : Boolean = args(3).toLowerCase.toBoolean
  val recommendationsPath : String = folder + args(4) + "_" + System.currentTimeMillis.toString
  val topK : Int = args(5).toInt

  val customersSizePath : String = customerItemPath + ".lookup.size"
  val itemsSizePath : String = itemItemPath + ".lookup.size"

  lazy val getSparkSession : SparkSession = {

    val sparkBuilder =
      if (folder.startsWith("hdfs:///")) SparkSession.builder
      else SparkSession.builder.master("local[*]")

    sparkBuilder
      .appName("SparkRecommenderThesis")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", "hr.fer.ztel.thesis.spark.SparkKryoRegistrator")
      .getOrCreate
  }

}