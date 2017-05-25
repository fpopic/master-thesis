package hr.fer.ztel.thesis.ml.multiplication.block


import hr.fer.ztel.thesis.datasource.MatrixEntryDataSoruce
import hr.fer.ztel.thesis.ml.CosineSimilarityMeasure
import hr.fer.ztel.thesis.spark.SparkSessionHolder
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD

object BlockMatrixMultiplication {

  def main(args : Array[String]) : Unit = {

    implicit val spark = SparkSessionHolder.getSession

    import SparkSessionHolder._

    val measure = new CosineSimilarityMeasure(normalize = true)

    val itemItemMatrix : RDD[MatrixEntry] =
      MatrixEntryDataSoruce.readItemItemEntries(itemItemInputPath, measure)

    val customerItemMatrix : RDD[MatrixEntry] =
      MatrixEntryDataSoruce.readCustomerItemEntries(customerItemInputPath)

    val numCustomers : Int = ??? // ili iz c++ ili nakon filtera qunatity >= 1  napraviti distinct
    val numItems : Int = ??? // mogu iz c++ lako O(1) lookup.size

    val rowsPerBlock = 1024
    val colsPerBlock = 1024

    val C = new CoordinateMatrix(customerItemMatrix, numCustomers, numItems)
      .toBlockMatrix(rowsPerBlock, colsPerBlock)

    val S = new CoordinateMatrix(itemItemMatrix, numItems, numItems)
      .toBlockMatrix(rowsPerBlock, colsPerBlock)

    import breeze.linalg.argtopk
    import org.apache.spark.mllib.linalg.MLlibBreezeExtensions._

    val topK = 5 // recommendations

    val R = (C multiply S)
      .toIndexedRowMatrix
      .rows
      .map(row => (row.index.toInt, argtopk(row.vector.toBreeze, topK)))

    //todo


  }

}
