package hr.fer.ztel.other

import org.apache.spark.sql.SparkSession

object RddMulA {

  object implicits {
    implicit def toDoubleArray(a : Array[Int]) : Array[Double] = a.map(_.toDouble)
  }

  def dot(a : Array[Double], b : Array[Double]) : Double = {
    a.zip(b)
      .map { case (m, n) => m * n }
      .sum
  }

  def main(args : Array[String]) : Unit = {

    val warehouse = System.getProperty("user.dir") + "\\spark-warehouse"

    val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.warehouse.dir", warehouse)
      .getOrCreate

    import RddMulA.implicits.toDoubleArray
    import spark.implicits._

    val C = spark.createDataset(Seq(
      (57, Array(1, 0, 1, 1, 1)),
      (13, Array(1, 0, 0, 0, 1)),
      (44, Array(0, 0, 0, 0, 1)),
      (11, Array(0, 1, 1, 1, 0))
    ))

    val S = spark.createDataset(Seq(
      (1, Array(1.0, 0.8, 0.3, 0.5, 0.5)),
      (2, Array(0.8, 1.0, 0.5, 0.3, 0.7)),
      (3, Array(0.3, 0.5, 1.0, 0.1, 0.8)),
      (4, Array(0.5, 0.3, 0.1, 1.0, 0.2)),
      (5, Array(0.5, 0.7, 0.8, 0.2, 1.0))
    ))

    // with cartesian you avoid nested rdds transformations
    val tripples = C.rdd.cartesian(S.rdd)
      .map {
        case ((userId, userVector), (itemId, itemVector)) => (userId, itemId, dot(userVector, itemVector))
      }
      .toDS

    tripples.show()

    // now you need groupByKey (by userId) to create user recommendation vectors

  }
}
