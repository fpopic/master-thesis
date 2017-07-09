package hr.fer.ztel.thesis.multiplication.outer

import hr.fer.ztel.thesis.datasource.MatrixEntryDataSource._
import hr.fer.ztel.thesis.spark.SparkSessionHandler
import hr.fer.ztel.thesis.sparse_linalg.SparseVectorOperators.argTopK
import org.apache.spark.HashPartitioner

object OuterMatrixEntry {

  def main(args: Array[String]): Unit = {

    val handler = new SparkSessionHandler(args)
    implicit val spark = handler.getSparkSession

    val partitioner = new HashPartitioner(128)

    val itemUserEntries = readItemUserEntries(handler.userItemPath)
      .map(x => (x.i.toInt, (x.j.toInt, x.value)))
      .partitionBy(partitioner)

    val itemItemEntries = readItemItemEntries(handler.itemItemPath, handler.measure, handler.normalize)
      .map(x => (x.i.toInt, (x.j.toInt, x.value)))
      .partitionBy(partitioner)

    val itemSeenByUsersBroadcast = spark.sparkContext.broadcast(
      itemUserEntries.groupByKey.mapValues(_.map(_._1).toSet).collectAsMap.toMap
    )

    val recommendations = itemUserEntries.join(itemItemEntries, partitioner)
      .map { case (_, ((user, _), (item, similarity))) => ((user, item), similarity) }
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
