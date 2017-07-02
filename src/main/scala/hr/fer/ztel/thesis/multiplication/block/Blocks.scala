package hr.fer.ztel.thesis.multiplication.block

import breeze.linalg.argtopk
import hr.fer.ztel.thesis.datasource.MatrixEntryDataSource._
import hr.fer.ztel.thesis.spark.SparkSessionHandler
import org.apache.spark.mllib.linalg.MLlibBreezeConversions._
import org.apache.spark.mllib.linalg.distributed.MLlibBlockMatrixMultiplyVersion220._
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}

object Blocks {

  def main(args: Array[String]): Unit = {

    val handler = new SparkSessionHandler(args)
    implicit val spark = handler.getSparkSession

    val userItemEntries = readUserItemEntries(handler.userItemPath)
    val itemItemEntries = readItemItemEntries(handler.itemItemPath, handler.measure, handler.normalize)

    // precomputed (max number, possbile that some users were filtered by quantity treshold after)
    val numUsers = spark.sparkContext.textFile(handler.usersSizePath, 1).first.toInt
    val numItems = spark.sparkContext.textFile(handler.itemsSizePath, 1).first.toInt

    val C = new CoordinateMatrix(userItemEntries, numUsers, numItems).toBlockMatrix()
    val S = new CoordinateMatrix(itemItemEntries, numItems, numItems).toBlockMatrix()

    val R = multiply(C, S)

    val userSeenItemsBroadcast = spark.sparkContext.broadcast(
      userItemEntries
        .map { case MatrixEntry(user, item, _) => (user.toInt, item.toInt) }
        .groupByKey.mapValues(_.toSet)
        .collectAsMap.toMap
    )

    val recommendations = R.toIndexedRowMatrix.rows
      .mapPartitions {
        val localUserSeenItems = userSeenItemsBroadcast.value
        _.filter(row => localUserSeenItems.contains(row.index.toInt))
          .map { row =>
            val user = row.index.toInt
            val unseenItems = argtopk(row.vector.toBreeze, handler.topK)
              .filterNot(item => localUserSeenItems(user).contains(item))

            s"$user:${unseenItems.mkString(",")}"
          }
      }

    recommendations.saveAsTextFile(handler.recommendationsPath)

    println(s"Recommendations saved in: ${handler.recommendationsPath}.")
  }
}