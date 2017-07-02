package hr.fer.ztel.thesis.multiplication.inner

import hr.fer.ztel.thesis.datasource.MatrixDataSource._
import hr.fer.ztel.thesis.measure.CosineSimilarityMeasure
import hr.fer.ztel.thesis.sparse_linalg.SparseVectorOperators._
import hr.fer.ztel.thesis.spark.SparkSessionHandler
import org.apache.spark.rdd.RDD

object InnerCartesianRdds {

  def main(args: Array[String]): Unit = {

    val handler = new SparkSessionHandler(args)
    implicit val spark = handler.getSparkSession

    val userItemMatrix = readUserItemMatrix(handler.userItemPath)
    val itemItemMatrix = readItemItemMatrix(handler.itemItemPath, handler.measure, handler.normalize)

    val userSeenItemsBroadcast = spark.sparkContext.broadcast(
      userItemMatrix.mapValues(_.toSet).collectAsMap().toMap
    )

    val recommendationMatrix = (userItemMatrix cartesian itemItemMatrix)
      .map { case ((user, userVector), (item, itemVector)) =>
        (user, (item, dot(userVector, itemVector)))
      }
      .groupByKey
      .mapPartitions {
        val localUserSeenItems = userSeenItemsBroadcast.value
        _.map { case (user, utilities) =>
          val items = argTopK(utilities.toArray, handler.topK)
            .filterNot(localUserSeenItems(user).contains(_))

          s"$user:${items.mkString(",")}"
        }
      }
    recommendationMatrix.saveAsTextFile(handler.recommendationsPath)

    println(s"Recommendations saved in: ${handler.recommendationsPath}.")
  }
}