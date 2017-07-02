package hr.fer.ztel.thesis

import hr.fer.ztel.thesis.datasource.MatrixEntryDataSource.readItemItemEntries
import hr.fer.ztel.thesis.spark.SparkSessionHandler
import org.apache.spark.mllib
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, IndexedRow}

object EvaluationMain {

  def main(args: Array[String]): Unit = {

    val handler = new SparkSessionHandler(args)
    implicit val spark = handler.getSparkSession

    val numItems = spark.sparkContext.textFile(handler.itemsSizePath, 1).first.toInt

    val itemItemEntries = readItemItemEntries(handler.itemItemPath, handler.measure, handler.normalize)

    val itemitemMatrix = new CoordinateMatrix(itemItemEntries, numItems, numItems).toIndexedRowMatrix

    //import org.apache.spark.mllib.linalg.MLlibBreezeConversions._

    itemitemMatrix.rows
      .map { case IndexedRow(item: Long, itemVector: mllib.linalg.Vector) =>
        val sb = new StringBuilder(s"$item,[")
        itemVector.foreachActive { (otherItem: Int, similarity: Double) =>
          sb.append("(" + otherItem + "," + similarity + "),")
        }
        sb.delete(sb.size - 1, sb.size).append("]").toString
      }
      .saveAsTextFile(handler.recommendationsPath)

  }

}