package hr.fer.ztel.thesis.ml.multiplication.inner

import hr.fer.ztel.thesis.datasource.MatrixDataSource._
import hr.fer.ztel.thesis.ml.SparseLinearAlgebra._
import hr.fer.ztel.thesis.ml.{CosineSimilarityMeasure, ItemPairSimilarityMeasure}
import org.apache.spark.sql.SparkSession

object MainDotCartesianRddWithBroadcastedMap {

  def main(args : Array[String]) : Unit = {

    implicit val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.warehouse.dir", "/media/fpopic/Data/spark-warehouse")
      .getOrCreate

    val measure = new CosineSimilarityMeasure

    val itemItemMatrix = createItemItemMatrix("src/main/resources/item_matrix_10.csv", measure)

    val customerItemMatrix = createCustomerItemMatrix("src/main/resources/transactions_10.csv")
      .collect

    val customerItemMatrixBroadcasted = spark.sparkContext.broadcast(customerItemMatrix)

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    val N = 5 // top n recommendations

    val recommendations = itemItemMatrix
      .mapPartitions(
        partition => {
          val localCustomerItemMatrix = customerItemMatrixBroadcasted.value
          partition.flatMap {
            case (itemId, itemVector) => localCustomerItemMatrix.map {
              case (customerId, customerVector) => (customerId, (itemId, dot(customerVector, itemVector)))
            }
          }
        }, preservesPartitioning = true)
      .groupByKey
      .mapValues(utilities => utilities.toArray.sortBy(_._2).takeRight(N).map(_._1))

    println(recommendations.count)

  }

}