package hr.fer.ztel.thesis.multiplication.outer

import hr.fer.ztel.thesis.datasource.MatrixDataSource.{readItemCustomerMatrix, readItemItemMatrix}
import hr.fer.ztel.thesis.ml.CosineSimilarityMeasure
import hr.fer.ztel.thesis.ml.SparseLinearAlgebra._
import hr.fer.ztel.thesis.spark.SparkSessionHandler

object OuterMapJoin {

  def main(args : Array[String]) : Unit = {

    val handler = new SparkSessionHandler(args)
    implicit val spark = handler.getSparkSession

    val measure = new CosineSimilarityMeasure(handler.normalize)

    val itemItemMatrix = readItemItemMatrix(handler.itemItemPath, measure)

    val itemCustomerMatrix = readItemCustomerMatrix(handler.customerItemPath)

    val itemCustomerMatrixBroadcasted = spark.sparkContext.broadcast(
      itemCustomerMatrix.collect
    )

    itemCustomerMatrix.unpersist(true)

    val k = handler.topK

    // map-join
    val recommendations = itemItemMatrix
      .mapPartitions({
        val localItemCustomerMap = itemCustomerMatrixBroadcasted.value.toMap
        _.flatMap {
          case (itemId, itemVector) =>
            val customerVector = localItemCustomerMap.get(itemId)
            if (customerVector.isDefined) {
              //println(s"Match for item: $itemId")
              outer(customerVector.get, itemVector)
            }
            else {
              //println(s"No match for item: $itemId")
              None
            }
        }
      })
      .reduceByKey(_ + _)
      .map {
        case ((customerId, itemId), utility) => (customerId, (itemId, utility))
      }
      .groupByKey
      .map {
        case (customer, utilities) =>
          val items = utilities.toArray.sortBy(_._2).map(_._1).takeRight(k)
          s"$customer:${items.mkString(",")}"
      }

    itemItemMatrix.unpersist()

    println(s"Recommendations: ${recommendations.count} saved in: ${handler.recommendationsPath}.")

    recommendations.saveAsTextFile(handler.recommendationsPath)

  }

}