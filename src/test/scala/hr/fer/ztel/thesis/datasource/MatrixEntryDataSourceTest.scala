package hr.fer.ztel.thesis.datasource

import hr.fer.ztel.thesis.measure.CosineSimilarityMeasure
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class MatrixEntryDataSourceTest extends FlatSpec with Matchers with BeforeAndAfter {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  implicit val spark: SparkSession = SparkSession.builder.master("local[*]").getOrCreate()

  before {
    spark.newSession()
  }

  after {
    System.clearProperty("spark.driver.port")
  }

  "readItemItemEntries" should "create item-item similarity normalized matrix" in {

    val itemItemPath = "src/test/resources/item_matrix.test"

    val actualItemItemEntries =
      MatrixEntryDataSource.readItemItemEntries(itemItemPath, new CosineSimilarityMeasure, normalizeRows = true)
        .map { case MatrixEntry(item, other, similarity) => (item.toInt, other.toInt, similarity) }
        .collect.sortBy(t => (t._1, t._2))

    //@formatter:off
    val expectedItemItemEntries = Array(
                    (0, 1, 0.53), (0, 2, 0.00), (0, 3, 0.46),
      (1, 0, 0.44),               (1, 2, 0.42), (1, 3, 0.12),
      (2, 0, 0.00), (2, 1, 0.52),               (2, 3, 0.47),
      (3, 0, 0.42), (3, 1, 0.14), (3, 2, 0.43)
    )
    //@formatter:on

    (actualItemItemEntries zip expectedItemItemEntries)
      .foreach { case (first, second) =>
        assert(first._1 === second._1 && first._2 === second._2 && (first._3 === second._3 +- 0.01))
      }

  }

  "readUserItemEntries" should "create user-item consumation matrix" in {

    val userItemPath = "src/test/resources/user_matrix.test"

    val actualUserItemEntries =
      MatrixEntryDataSource.readUserItemEntries(userItemPath)
        .map { case MatrixEntry(user, item, quantity) => (user.toInt, item.toInt, quantity) }
        .collect.sortBy(t => (t._1, t._2))

    //@formatter:off
    val expectedUserItemEntries = Array(
      (0, 0, 1.00),               (0, 2, 1.00),
      (1, 0, 1.00), (1, 1, 1.00),
                    (2, 1, 1.00),              (2, 3, 1.00)
    )
    //@formatter:on

    (actualUserItemEntries zip expectedUserItemEntries)
      .foreach { case (act, exp) =>
        assert(act._1 === exp._1 && act._2 === exp._2 && act._3 === exp._3 +- 0.01)
      }

  }

}
