package hr.fer.ztel.dipl.run.inner

import hr.fer.ztel.dipl.datasource.MatrixDataSource
import hr.fer.ztel.dipl.ml.{CosineSimiliarityMeasure, ItemPairSimiliarityMeasure}
import hr.fer.ztel.dipl.ml.SparseLinearAlgebra._
import hr.fer.ztel.dipl.model._
import org.apache.spark.sql.SparkSession

object MainDotCartesianRddWithBroadcast {

  def main(args : Array[String]) : Unit = {

    implicit val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.warehouse.dir", "/media/fpopic/Data/spark-warehouse")
      .getOrCreate

    val measure : ItemPairSimiliarityMeasure = new CosineSimiliarityMeasure

    val itemItemMatrix = MatrixDataSource.createItemItemMatrix("src/main/resources/item_matrix_10.csv", measure)

    val customerItemMatrix = MatrixDataSource.createCustomerItemMatrix("src/main/resources/transactions_10.csv")
      .collect

    val customerItemMatrixBroadcasted = spark.sparkContext.broadcast(customerItemMatrix)

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    val N = 5 // top n recommendations

    val recommendations = itemItemMatrix.mapPartitions({ partition =>
        val localCustomerItemMatrix = customerItemMatrixBroadcasted.value
        partition.map {
          case (itemId, itemVector) => localCustomerItemMatrix.map {
            case (customerId, customerVector) => (customerId, (itemId, dot(customerVector, itemVector)))
          }.toSeq
        }.flatten
      }, preservesPartitioning = true)
      .groupByKey
      .mapValues(utilities => utilities.toSeq.sortBy(_._2).takeRight(N).map(_._1))

    println(recommendations.count)

  }

}