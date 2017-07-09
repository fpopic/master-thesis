package hr.fer.ztel.thesis.multiplication.outer

import hr.fer.ztel.thesis.datasource.MatrixDataSource.{readBoughtItemItemMatrix, readItemUserMatrix}
import hr.fer.ztel.thesis.spark.SparkSessionHandler
import hr.fer.ztel.thesis.sparse_linalg.SparseVectorOperators._
import org.apache.spark.rdd.RDD

object OuterMapJoin {

  def main(args: Array[String]): Unit = {

    val handler = new SparkSessionHandler(args)
    implicit val spark = handler.getSparkSession

    val itemUserMatrix = readItemUserMatrix(handler.userItemPath)

    // to reduce join (shuffle size), discarding all unmatched rows/cols in item-item matrix
    val broadcastedBoughtItems = spark.sparkContext.broadcast(itemUserMatrix.keys.collect)

    val boughtItemItemMatrix = // (item, [item, sim])
      readBoughtItemItemMatrix(handler.itemItemPath, handler.measure, handler.normalize, broadcastedBoughtItems)

    val itemUserMatrixBroadcasted = spark.sparkContext.broadcast(itemUserMatrix.mapValues(_.toSet).collectAsMap.toMap)

    val recommendations = boughtItemItemMatrix
      .mapPartitions {
        val localItemUserMap = itemUserMatrixBroadcasted.value
        _.flatMap { case (item, itemVector) =>
          // map-join
          val userVector = localItemUserMap.get(item)
          if (userVector.isDefined) {
            //println(s"Match for item: $item")
            outer(userVector.get, itemVector)
          }
          else {
            //println(s"No match for item: $item")
            None
          }
        }
      }
      .reduceByKey(_ + _)
      .map { case ((user, item), utility) => (user, (item, utility))
      }
      .groupByKey
      .mapPartitions {
        val localItemSeenByUsers = itemUserMatrixBroadcasted.value
        _.map { case (user, items) =>
          val unSeenItems = items.filterNot { case (item, _) => localItemSeenByUsers(item).contains(user) }
          val unSeenTopKItems = argTopK(unSeenItems.toArray, handler.topK)

          s"$user:${unSeenTopKItems.mkString(",")}"
        }
      }

    println(s"Recommendations saved in: ${handler.recommendationsPath}.")

    recommendations.saveAsTextFile(handler.recommendationsPath)
  }
}