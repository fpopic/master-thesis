package hr.fer.ztel.dipl.run.outer

import hr.fer.ztel.dipl.model._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object MainOuterRdds {

  def main(args : Array[String]) : Unit = {

    implicit val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.warehouse.dir", "/media/fpopic/Data/spark-warehouse")
      .getOrCreate

    val measure : ItemPairSimiliarityMeasure = new CosineSimiliarityMeasure

    val itemItemMatrix : RDD[(Int, Map[Int, Double])] = DataSource.createItemItemMatrix("src/main/resources/item_matrix_10.csv", measure)

    val customerItemMatrix : RDD[(Int, Set[Int])] = DataSource.createCustomerItemMatrix("src/main/resources/transactions_10.csv")





  }
}