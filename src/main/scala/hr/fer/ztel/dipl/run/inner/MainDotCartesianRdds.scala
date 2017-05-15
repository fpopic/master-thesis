package hr.fer.ztel.dipl.run.inner

import hr.fer.ztel.dipl.model.SparseVectorAlgebra._
import hr.fer.ztel.dipl.model._
import org.apache.spark.sql.SparkSession

object MainDotCartesianRdds {

  def main(args : Array[String]) : Unit = {

    implicit val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.warehouse.dir", "/media/fpopic/Data/spark-warehouse")
      .getOrCreate

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "400000000")

    val measure : ItemPairSimiliarityMeasure = new CosineSimiliarityMeasure

    val itemItemMatrix = DataSource.createItemItemMatrix("src/main/resources/item_matrix_10.csv", measure)

    val customerItemMatrix = DataSource.createCustomerItemMatrix("src/main/resources/transactions_10.csv")

    val N = 5 // top n recommendations

    val recommendationMatrix = (customerItemMatrix cartesian itemItemMatrix)
      .map {
        case ((customerId, customerVector), (itemId, itemVector)) => (customerId, (itemId, dot(customerVector, itemVector)))
      }
      .groupByKey
      .map {
        case (customerId, utilities) => (customerId, utilities.toSeq.sortBy(_._2).takeRight(N).map(_._1))
      }

    println(recommendationMatrix.count)

  }

}
