package hr.fer.ztel.thesis.multiplication.block

import breeze.linalg.argtopk
import hr.fer.ztel.thesis.datasource.MatrixEntryDataSource._
import hr.fer.ztel.thesis.spark.SparkSessionHandler
import org.apache.spark.mllib.linalg.MLlibBreezeConversions._
import org.apache.spark.mllib.linalg.distributed.BlockMatrixMultiply220._
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}

object BlocksMultiplication {

  def main(args: Array[String]): Unit = {

    val handler = new SparkSessionHandler(args)
    implicit val spark = handler.getSparkSession

    val userItemEntries = readUserItemEntries(handler.userItemPath)
    val itemItemEntries = readItemItemEntries(handler.itemItemPath, handler.measure)

    // precomputed
    val numUsers = spark.sparkContext.textFile(handler.usersSizePath, 1).first.toInt
    val numItems = spark.sparkContext.textFile(handler.itemsSizePath, 1).first.toInt

    println(s"Total number of users: $numUsers")
    println(s"Total number of items: $numItems")

    val C = new CoordinateMatrix(userItemEntries, numUsers, numItems).toBlockMatrix()
    val S = new CoordinateMatrix(itemItemEntries, numItems, numItems).toBlockMatrix()

    val R = multiply(C, S)

    C.blocks.unpersist(false)
    S.blocks.unpersist(false)

    val userSeenItems = userItemEntries
      .map { case MatrixEntry(user, item, _) => (user, item) }
      .groupByKey
      .map { case (user, seenItems) => (user, seenItems.toSet) }
      .collectAsMap
      .toMap

    val userSeenItemsBroadcast = spark.sparkContext.broadcast(userSeenItems)

    val recommendations = R.toIndexedRowMatrix.rows
      .mapPartitions {
        val localUserSeenItems = userSeenItemsBroadcast.value
        _.filter(row => localUserSeenItems.contains(row.index))
          .map { row =>
            val user = row.index
            val unseenItems = argtopk(row.vector.toBreeze, handler.topK)
              .filterNot(item => localUserSeenItems(user).contains(item.toLong))

            s"$user:${unseenItems.mkString(",")}"
          }
      }

    recommendations.saveAsTextFile(handler.recommendationsPath)
  }
}