package hr.fer.ztel.thesis.multiplication.outer

import hr.fer.ztel.thesis.datasource.MatrixEntryDataSource
import hr.fer.ztel.thesis.ml.CosineSimilarityMeasure
import hr.fer.ztel.thesis.spark.SparkSessionHandler

object OuterMatrixEntry {

  def main(args: Array[String]): Unit = {

    val handler = new SparkSessionHandler(args)
    implicit val spark = handler.getSparkSession

    val measure = new CosineSimilarityMeasure(false)

    val userItemEntries = MatrixEntryDataSource.readUserItemEntries(handler.userItemPath)
      .map(x => (x.j, (x.i, x.value)))

    val itemItemEntries = MatrixEntryDataSource.readItemItemEntries(handler.itemItemPath, measure)
      .map(x => (x.i, (x.j, x.value)))

    val k = handler.topK

    val recommendations = (userItemEntries join itemItemEntries)
      .map { case (_, ((user, _), (item, similarity))) => ((user, item), similarity) }
      // by (user, item) key
      .reduceByKey(_ + _)
      .map { case ((user, item), utility) => (user, (item, utility)) }
      // by (user) key
      .groupByKey
      .map { case (user, utilities) =>
        val items = utilities.toArray.sortBy(_._2).map(_._1).takeRight(k)
        s"$user:${items.mkString(",")}"
      }

    recommendations.saveAsTextFile(handler.recommendationsPath)

    println(s"Recommendations: ${recommendations.count} saved in: ${handler.recommendationsPath}.")

  }
}
