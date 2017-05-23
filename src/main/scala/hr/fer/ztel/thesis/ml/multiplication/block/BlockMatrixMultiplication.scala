package hr.fer.ztel.thesis.ml.multiplication.block

import hr.fer.ztel.thesis.datasource.IndexedMatrixDataSource._
import hr.fer.ztel.thesis.ml.CosineSimilarityMeasure
import hr.fer.ztel.thesis.spark.SparkSessionHolder
import hr.fer.ztel.thesis.spark.SparkSessionHolder._
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD

object BlockMatrixMultiplication {

  def main(args : Array[String]) : Unit = {

    implicit val spark = SparkSessionHolder.getSession

    val itemsLookup = persistItemIdIndexLookup(itemItemInputPath, ItemsLookupPath)
    val customersLookup = persistCustomerIdIndexLookup(customerItemInputPath, customersLookupPath)

    val partitioner = Some(new HashPartitioner(16))
    val measure = new CosineSimilarityMeasure

    val itemItemMatrix : RDD[(Int, Map[Int, Double])] =
      createItemItemMatrix(itemItemInputPath, measure, itemsLookup, partitioner)

    val itemCustomerMatrix : RDD[(Int, Array[Int])] =
      createItemCustomerMatrix(customerItemInputPath, itemsLookup, customersLookup, partitioner)

    // sad raditi sa ove matrice a tek na kraju vratiti id-eve


  }

}
