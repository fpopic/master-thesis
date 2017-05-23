package hr.fer.ztel.thesis.ml.multiplication.outer

import hr.fer.ztel.thesis.datasource.MatrixDataSource
import hr.fer.ztel.thesis.datasource.MatrixDataSource._
import hr.fer.ztel.thesis.ml.CosineSimilarityMeasure
import hr.fer.ztel.thesis.ml.SparseLinearAlgebra._
import hr.fer.ztel.thesis.spark.SparkSessionHolder
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD

object MainOuterRdds {

  def main(args : Array[String]) : Unit = {

    implicit val spark = SparkSessionHolder.getSession

    val measure = new CosineSimilarityMeasure

    val partitioner = Some(new HashPartitioner(32))

    val itemCustomerMatrix : RDD[(Int, Array[Int])] = // (item, customers)
      createItemCustomerMatrix("src/main/resources/transactions_10.csv", partitioner)

    // to reduce join (shuffle) size, discarding all unmatched rows/cols in item-item matrix
    val broadcastedBoughtItems = spark.sparkContext.broadcast(itemCustomerMatrix.keys.collect)

    val boughtItemItemMatrix : RDD[(Int, Map[Int, Double])] = // (item, item, sims)
      createBoughtItemItemMatrix("src/main/resources/item_matrix_10.csv", measure, broadcastedBoughtItems, partitioner)

    val N = 5

    // join should produce at most 40k out of 67k tuples in shuffle-u
    val recommendations = itemCustomerMatrix.join(boughtItemItemMatrix, partitioner.get)
      .mapPartitions(
        _.flatMap {
          case (_, (customerVector, itemVector)) => outer(customerVector, itemVector)
        }, preservesPartitioning = true)
      // by (customer,item) key
      .reduceByKey(_ + _)
      .map {
        case ((customerId, itemId), utility) => (customerId, (itemId, utility))
      }
      // by customer key
      .groupByKey
      .mapValues(utilities => utilities.toArray.sortBy(_._2).map(_._1).takeRight(N))

    recommendations.count()

  }
}