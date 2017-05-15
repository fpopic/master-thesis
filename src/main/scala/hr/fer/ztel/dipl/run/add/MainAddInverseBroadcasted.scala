package hr.fer.ztel.dipl.run.add

import hr.fer.ztel.dipl.model.SparseVectorAlgebra.add
import hr.fer.ztel.dipl.model._
import org.apache.spark.sql.SparkSession

object MainAddInverseBroadcasted {

  def main(args : Array[String]) : Unit = {

    implicit val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.warehouse.dir", "/media/fpopic/Data/spark-warehouse")
      .getOrCreate

    val measure : ItemPairSimiliarityMeasure = new CosineSimiliarityMeasure

    val itemItemMatrix = DataSource.createItemItemMatrix("src/main/resources/item_matrix_10.csv", measure)

    val itemCustomerMatrix = DataSource.createItemCustomerMatrix("src/main/resources/transactions_10.csv")
      .collectAsMap.toMap

    val itemCustomerMatrixBroadcasted = spark.sparkContext.broadcast(itemCustomerMatrix)

    val partialVectors = itemItemMatrix
      .flatMap {
        case (itemId, itemVector) =>
          itemCustomerMatrixBroadcasted.value(itemId).map(customerId => (customerId, itemVector)).toSeq
      }
      .reduceByKey(add)

    val N = 5 //top n recommendations

    // reduce at workers
    val recommendationMatrix = partialVectors
      .map {
        case (customerId, utilities) => (customerId, utilities.toSeq.sortBy(_._2).map(_._1).takeRight(N))
      }

    recommendationMatrix.foreach(println)

  }

}