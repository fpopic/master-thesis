package hr.fer.ztel.thesis.spark

import org.apache.spark.sql.SparkSession

object SparkSessionHolder extends Serializable {

  val warehouse : String = "/media/fpopic/Data/spark-warehouse"

  val itemItemInputPath : String = warehouse + "/item_matrix.csv"
  val customerItemInputPath : String = warehouse + "/customer_matrix.csv"

  val ItemsLookupPath : String = warehouse + "/item_matrix_lookup.csv"
  val customersLookupPath : String = warehouse + "/customer_matrix_lookup.csv"

  val getSession : SparkSession = SparkSession.builder
    .master("local[*]")
    .appName("spark-recommender-thesis")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.warehouse.dir", warehouse)
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.kryo.registrator", "hr.fer.ztel.datasource.SparseKryoRegistrator")
    .getOrCreate

}