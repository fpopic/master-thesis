package hr.fer.ztel.thesis.multiplication.block

import hr.fer.ztel.thesis.datasource.MatrixEntryDataSource
import hr.fer.ztel.thesis.ml.CosineSimilarityMeasure
import hr.fer.ztel.thesis.spark.SparkSessionHandler
import org.apache.spark.mllib.linalg.distributed.{BlockMatrix, CoordinateMatrix, IndexedRow, MatrixEntry}
import org.apache.spark.rdd.RDD

object SubMatricesMultiplication {

  def main(args: Array[String]): Unit = {

    val handler = new SparkSessionHandler(args)
    implicit val spark = handler.getSparkSession

    val measure = new CosineSimilarityMeasure(handler.normalize)

    val userItemEntries: RDD[MatrixEntry] = MatrixEntryDataSource.readUserItemEntries(handler.userItemPath)
    val itemItemEntries: RDD[MatrixEntry] = MatrixEntryDataSource.readItemItemEntries(handler.itemItemPath, measure)

    // precomputed
    val numUsers = spark.sparkContext.textFile(handler.usersSizePath, 1).first.toInt
    val numItems = spark.sparkContext.textFile(handler.itemsSizePath, 1).first.toInt

    println("Users: " + numUsers)
    println("Items: " + numItems)

    val C: BlockMatrix = new CoordinateMatrix(userItemEntries, numUsers, numItems).toBlockMatrix
    val S = new CoordinateMatrix(itemItemEntries, numItems, numItems).toBlockMatrix

    userItemEntries.unpersist(true)
    itemItemEntries.unpersist(true)

    import breeze.linalg.argtopk
    import org.apache.spark.mllib.linalg.MLlibBreezeConversions._

    val k = handler.topK

    val mul = C multiply S

    C.blocks.unpersist(true)
    S.blocks.unpersist(true)

    val recommendations: RDD[String] = mul.toIndexedRowMatrix().rows
      .map { row =>
        val user = row.index.toInt
        val items = argtopk(row.vector.toBreeze, k)
        s"$user:${items.mkString(",")}"
      }

    recommendations.saveAsTextFile(handler.recommendationsPath)

    println(s"Recommendations: ${recommendations.count} saved " +
      s"in: ${handler.recommendationsPath}.")

  }

}