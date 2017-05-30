package hr.fer.ztel.thesis.multiplication.inner

import hr.fer.ztel.thesis.datasource.MatrixDataSource._
import hr.fer.ztel.thesis.ml.CosineSimilarityMeasure
import hr.fer.ztel.thesis.ml.SparseLinearAlgebra._
import hr.fer.ztel.thesis.spark.SparkSessionHandler

object InnerCartesianRdds {

  def main(args : Array[String]) : Unit = {

    val handler = new SparkSessionHandler(args)
    implicit val spark = handler.getSparkSession

    val measure = new CosineSimilarityMeasure(handler.normalize)

    val itemItemMatrix = readItemItemMatrix(handler.itemItemPath, measure)

    val customerItemMatrix = readCustomerItemMatrix(handler.customerItemPath)

    val k = handler.topK

    val recommendations = (customerItemMatrix cartesian itemItemMatrix)
      .map {
        case ((customer, customerVector), (item, itemVector)) =>
          (customer, (item, dot(customerVector, itemVector)))
      }
      .groupByKey
      .map {
        case (customer, utilities) =>
          val items = utilities.toArray.sortBy(_._2).map(_._1).takeRight(k)
          s"$customer:${items.mkString(",")}"
      }

    recommendations.saveAsTextFile(handler.recommendationsPath)

    println(s"Recommendations: ${recommendations.count} saved in: ${handler.recommendationsPath}.")

  }

}
