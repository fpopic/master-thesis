package hr.fer.ztel.thesis.multiplication.outer

import hr.fer.ztel.thesis.datasource.MatrixDataSource.{readBoughtItemItemMatrix, readItemUserMatrix}
import hr.fer.ztel.thesis.ml.CosineSimilarityMeasure
import hr.fer.ztel.thesis.ml.SparseVectorOperators._
import hr.fer.ztel.thesis.spark.SparkSessionHandler
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object OuterMapJoin {

  def main(args: Array[String]): Unit = {

    val handler = new SparkSessionHandler(args)
    implicit val spark = handler.getSparkSession

    val measure = new CosineSimilarityMeasure(handler.normalize)

    val itemUserMatrix = readItemUserMatrix(handler.userItemPath)

    // to reduce join (shuffle size), discarding all unmatched rows/cols in item-item matrix
    val broadcastedBoughtItems = spark.sparkContext.broadcast(itemUserMatrix.keys.collect)

    val boughtItemItemMatrix: RDD[(Int, Map[Int, Double])] = // (item, [item, sim])
      readBoughtItemItemMatrix(handler.itemItemPath, measure, broadcastedBoughtItems)

    broadcastedBoughtItems.unpersist()

    val itemUserMatrixBroadcasted: Broadcast[Array[(Int, Array[Int])]] =
      spark.sparkContext.broadcast(itemUserMatrix.collect)

    itemUserMatrix.unpersist(true)

    val k = handler.topK

    val recommendations = boughtItemItemMatrix
      .mapPartitions {
        val localItemUserMap = itemUserMatrixBroadcasted.value.toMap
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
      .map { case (user, utilities) =>
        val items = utilities.toArray.sortBy(_._2).map(_._1).takeRight(k)
        s"$user:${items.mkString(",")}"
      }

    boughtItemItemMatrix.unpersist()

    println(s"Recommendations: ${recommendations.count} saved in: ${handler.recommendationsPath}.")

    recommendations.saveAsTextFile(handler.recommendationsPath)

  }

}