package hr.fer.ztel.thesis.multiplication.outer

import hr.fer.ztel.thesis.datasource.MatrixDataSource._
import hr.fer.ztel.thesis.ml.CosineSimilarityMeasure
import hr.fer.ztel.thesis.ml.SparseLinearAlgebra._
import hr.fer.ztel.thesis.spark.SparkSessionHandler
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

object OuterRdds {

  def main(args : Array[String]) : Unit = {

    val handler = new SparkSessionHandler(args)
    implicit val spark : SparkSession = handler.getSparkSession

    val measure = new CosineSimilarityMeasure(handler.normalize)
    val partitioner = Some(new HashPartitioner(16))

    val itemCustomerMatrix : RDD[(Int, Array[Int])] = // (item, [customer])
      readItemCustomerMatrix(handler.customerItemPath, partitioner)

    // to reduce join (shuffle size), discarding all unmatched rows/cols in item-item matrix
    val broadcastedBoughtItems = spark.sparkContext.broadcast(itemCustomerMatrix.keys.collect)

    val boughtItemItemMatrix : RDD[(Int, Map[Int, Double])] = // (item, [item, sim])
      readBoughtItemItemMatrix(handler.itemItemPath, measure, broadcastedBoughtItems, partitioner)

    val k = handler.topK

    broadcastedBoughtItems.unpersist()

    // join should produce at most 40k out of 67k tuples in shuffle-u
    val join = itemCustomerMatrix.join(boughtItemItemMatrix, partitioner.get)

    itemCustomerMatrix.unpersist()
    boughtItemItemMatrix.unpersist()

    val recommendations = join
      .flatMap {
        case (_, (customerVector, itemVector)) => outer(customerVector, itemVector)
      }
      // by (customer, item) key
      .reduceByKey(_ + _)
      .map {
        case ((customerId, itemId), utility) => (customerId, (itemId, utility))
      }
      // by (customer) key
      .groupByKey
      .map {
        case (customer, utilities) =>
          val items = utilities.toArray.sortBy(_._2).map(_._1).takeRight(k)
          s"$customer:${items.mkString(",")}"
      }

    recommendations.saveAsTextFile(handler.recommendationsPath)

    println(s"Recommendations: ${recommendations.count} saved in: ${handler.recommendationsPath}.")

  }
}