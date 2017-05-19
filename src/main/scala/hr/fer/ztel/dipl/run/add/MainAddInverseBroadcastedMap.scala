package hr.fer.ztel.dipl.run.add

import hr.fer.ztel.dipl.ml.{CosineSimiliarityMeasure, ItemPairSimiliarityMeasure}
import hr.fer.ztel.dipl.ml.SparseVectorAlgebra.addV
import hr.fer.ztel.dipl.model._
import org.apache.spark.sql.SparkSession

object MainAddInverseBroadcastedMap {

  def main(args : Array[String]) : Unit = {

    implicit val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.warehouse.dir", "/media/fpopic/Data/spark-warehouse")
      .getOrCreate

    val measure : ItemPairSimiliarityMeasure = new CosineSimiliarityMeasure

    val itemItemMatrix = MatrixDataSource.createItemItemMatrix("src/main/resources/item_matrix_10.csv", measure)

    val itemCustomerMatrix = MatrixDataSource.createItemCustomerMatrix("src/main/resources/transactions_10.csv")
      .collectAsMap.toMap

    val itemCustomerMatrixBroadcasted = spark.sparkContext.broadcast(itemCustomerMatrix)

    val N = 5 //top n recommendations

    // reduce at workers
    val recommendationMatrix = itemItemMatrix
      .mapPartitions(partition => {
        val itemCustomerMap = itemCustomerMatrixBroadcasted.value
        partition.flatMap {
          case (itemId, itemVector) =>
            itemCustomerMap(itemId).map(customerId => (customerId, itemVector)).toSeq
        }
      })
      .reduceByKey(addV)
      .mapValues(utilities => utilities.toSeq.sortBy(_._2).map(_._1).takeRight(N))

    recommendationMatrix.foreach(println)

  }

}