package hr.fer.ztel.thesis.multiplication.outer

import hr.fer.ztel.thesis.datasource.MatrixEntryDataSource._
import hr.fer.ztel.thesis.spark.SparkSessionHandler
import hr.fer.ztel.thesis.sparse_linalg.SparseVectorOperators.argTopK

object OuterMatrixEntry {

  def main(args: Array[String]): Unit = {

    val handler = new SparkSessionHandler(args)
    implicit val spark = handler.getSparkSession

    val itemUserEntries = readItemUserEntries(handler.userItemPath)
      .map(x => (x.i.toInt, (x.j.toInt, x.value)))

    val itemItemEntries = readItemItemEntries(handler.itemItemPath, handler.measure, handler.normalize)
      .map(x => (x.i.toInt, (x.j.toInt, x.value)))

    val itemSeenByUsersBroadcast = spark.sparkContext.broadcast(
      itemUserEntries.groupByKey.mapValues(_.map(_._1).toSet).collectAsMap.toMap
    )

    val recommendations = (itemUserEntries join itemItemEntries)
      .map { case (_, ((user, _), (item, similarity))) => ((user, item), similarity) }
      // by (user, item) key
      .reduceByKey(_ + _)
      .map { case ((user, item), utility) => (user, (item, utility)) }
      // by (user) key
      .groupByKey
      .mapPartitions {
        val localItemSeenByUsers = itemSeenByUsersBroadcast.value
        _.map { case (user, utilities) =>
          val unseenItems = argTopK(utilities.toArray, handler.topK)
            .filterNot(localItemSeenByUsers(_).contains(user))

          s"$user:${unseenItems.mkString(",")}"
        }
      }

    recommendations.saveAsTextFile(handler.recommendationsPath)

    println(s"Recommendations: ${recommendations.count} saved in: ${handler.recommendationsPath}.")

  }
}
