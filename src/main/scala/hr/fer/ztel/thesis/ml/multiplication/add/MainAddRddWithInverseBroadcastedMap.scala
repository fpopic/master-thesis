package hr.fer.ztel.thesis.ml.multiplication.add

import hr.fer.ztel.thesis.datasource.MatrixDataSource._
import hr.fer.ztel.thesis.ml.SparseLinearAlgebra.addV
import hr.fer.ztel.thesis.ml.{CosineSimilarityMeasure, ItemPairSimilarityMeasure}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

object MainAddRddWithInverseBroadcastedMap {

  def main(args : Array[String]) : Unit = {

    implicit val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.warehouse.dir", "/media/fpopic/Data/spark-warehouse")
      .getOrCreate

    val measure = new CosineSimilarityMeasure

    val itemItemMatrix = createItemItemMatrix("src/main/resources/item_matrix_10.csv", measure)

    val itemCustomerMatrix = createItemCustomerMatrix("src/main/resources/transactions_10.csv")
      .collectAsMap.toMap

    val itemCustomerMatrixBroadcasted : Broadcast[Map[Int, Array[Int]]] = spark.sparkContext.broadcast(itemCustomerMatrix)


    val N = 5 //top n recommendations

    // reduce at workers
    val recommendationMatrix = itemItemMatrix
      .mapPartitions(partition => {
        val localItemCustomerMap = itemCustomerMatrixBroadcasted.value
        partition.flatMap {
          case (itemId, itemVector) =>
            val customerVector = localItemCustomerMap.get(itemId)
            customerVector.map(customerId => (customerId, itemVector))
        }
      })
      .reduceByKey(addV)
      .mapValues(utilities => utilities.toArray.sortBy(_._2).map(_._1).takeRight(N))

    recommendationMatrix.foreach(println)

  }

}