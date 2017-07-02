package hr.fer.ztel.thesis.multiplication.block

import breeze.linalg.argtopk
import hr.fer.ztel.thesis.datasource.MatrixEntryDataSource.{readItemItemEntries, readUserItemEntries}
import hr.fer.ztel.thesis.measure.CosineSimilarityMeasure
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.MLlibBreezeConversions._
import org.apache.spark.mllib.linalg.distributed.MLlibBlockMatrixMultiplyVersion220._
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class BlocksTest extends FlatSpec with Matchers with BeforeAndAfter {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  implicit val spark: SparkSession =
    SparkSession.builder.master("local[*]").getOrCreate()

  before {
    spark.newSession()
  }

  after {
    System.clearProperty("spark.driver.port")
  }

  "Multiplication" should "return good value." in {

    val userItemPath = "src/test/resources/user_matrix.test"
    val itemItemPath = "src/test/resources/item_matrix.test"

    val userItemEntries = readUserItemEntries(userItemPath)
    val itemItemEntries = readItemItemEntries(itemItemPath, new CosineSimilarityMeasure, normalizeRows = true)

    val numUsers = 3
    val numItems = 4
    val topK = 2

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

    val actualRecommendations = R.toIndexedRowMatrix.rows
      .mapPartitions {
        val localUserSeenItems = userSeenItemsBroadcast.value
        _.filter(row => localUserSeenItems.contains(row.index))
          .map { row =>
            val user = row.index
            val unseenItems = argtopk(row.vector.toBreeze, topK)
              .filterNot(item => localUserSeenItems(user).contains(item.toLong))

            s"$user:${unseenItems.mkString(",")}"
          }
      }.collect.sorted

    val expectedRecommendations = Array(
      "0:1,3",
      "1:3",
      "2:0,2"
    ).sorted

    actualRecommendations shouldEqual expectedRecommendations
  }

}