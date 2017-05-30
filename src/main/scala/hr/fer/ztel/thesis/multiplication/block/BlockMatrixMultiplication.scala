package hr.fer.ztel.thesis.multiplication.block


import hr.fer.ztel.thesis.datasource.MatrixEntryDataSoruce
import hr.fer.ztel.thesis.ml.CosineSimilarityMeasure
import hr.fer.ztel.thesis.spark.SparkSessionHandler
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix
import org.apache.spark.sql.SaveMode

object BlockMatrixMultiplication {

  def main(args : Array[String]) : Unit = {

    if (args.length != 4) {
      println(SparkSessionHandler.argsMessage)
      System exit 1
    }

    val handler = new SparkSessionHandler(args)
    implicit val spark = handler.getSparkSession

    val measure = new CosineSimilarityMeasure(normalize = true)

    val customerItemEntries = MatrixEntryDataSoruce.readCustomerItemEntries(handler.customerItemPath)
    val itemItemEntries = MatrixEntryDataSoruce.readItemItemEntries(handler.itemItemPath, measure)

    // ako se koriste manji datasetovi potrebno ih indeksirati ispocetka
    val numCustomers = spark.sparkContext.textFile(handler.customersSizePath, 1).first.toInt
    val numItems = spark.sparkContext.textFile(handler.itemsSizePath, 1).first.toInt

    println("Customers: " + numCustomers)
    println("Items: " + numItems)

    val C = new CoordinateMatrix(customerItemEntries, numCustomers, numItems).toBlockMatrix
    val S = new CoordinateMatrix(itemItemEntries, numItems, numItems).toBlockMatrix

    customerItemEntries.unpersist(true)
    itemItemEntries.unpersist(true)

    import breeze.linalg.argtopk
    import org.apache.spark.mllib.linalg.MLlibBreezeExtensions._

    val k = handler.topK

    val mul = C multiply S

    C.blocks.unpersist(true)
    S.blocks.unpersist(true)

    val recommendations = mul.toIndexedRowMatrix.rows
      .map(row => {
        val customer = row.index.toInt
        val items = argtopk(row.vector.toBreeze, k)
        s"$customer:${items.mkString("", ",", "\n")}"
      })

    recommendations.saveAsTextFile(handler.recommendationsPath)

    println(s"Recommendations: ${recommendations.count} saved in: ${handler.recommendationsPath}.")

  }

}
