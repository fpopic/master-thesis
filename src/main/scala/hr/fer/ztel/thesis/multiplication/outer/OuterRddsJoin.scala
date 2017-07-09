package hr.fer.ztel.thesis.multiplication.outer

import hr.fer.ztel.thesis.datasource.MatrixDataSource._
import hr.fer.ztel.thesis.spark.SparkSessionHandler
import hr.fer.ztel.thesis.sparse_linalg.SparseVectorOperators._
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD

object OuterRddsJoin {

  def main(args: Array[String]): Unit = {

    val handler = new SparkSessionHandler(args)
    implicit val spark = handler.getSparkSession

    val partitioner = Some(new HashPartitioner(16))

    val itemUserMatrix: RDD[(Int, Array[Int])] = // (item, [user])
      readItemUserMatrix(handler.userItemPath, partitioner)

    // to reduce join (shuffle size), discarding all unmatched rows/cols in item-item matrix
    val broadcastedBoughtItems = spark.sparkContext.broadcast(itemUserMatrix.keys.collect)

    val boughtItemItemMatrix: RDD[(Int, Map[Int, Double])] = // (item, [item, sim])
      readBoughtItemItemMatrix(handler.itemItemPath, handler.measure, handler.normalize, broadcastedBoughtItems, partitioner)

    // join should produce at most 40k out of 67k tuples in shuffle-u
    val join = itemUserMatrix.join(boughtItemItemMatrix, partitioner.get)

    val itemSeenByUsersBroadcast = spark.sparkContext.broadcast(
      itemUserMatrix.mapValues(_.toSet).collectAsMap.toMap
    )

    val recommendations = join
      .flatMap { case (_, (userVector, itemVector)) => outer(userVector, itemVector) }
      // by (user, item) key
      .reduceByKey(_ + _)
      .map { case ((user, item), utility) => (user, (item, utility)) }
      // by (user) key
      .groupByKey
      .mapPartitions {
        val localItemSeenByUsers = itemSeenByUsersBroadcast.value
        _.map { case (user, items) =>
          val unSeenItems = items.filterNot { case (item, _) => localItemSeenByUsers(item).contains(user) }
          val unSeenTopKItems = argTopK(unSeenItems.toArray, handler.topK)

          s"$user:${unSeenTopKItems.mkString(",")}"
        }
      }

    recommendations.saveAsTextFile(handler.recommendationsPath)

    println(s"Recommendations saved in: ${handler.recommendationsPath}.")
  }
}