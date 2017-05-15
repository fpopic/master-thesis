package hr.fer.ztel.other

import breeze.linalg.DenseVector
import org.apache.spark.sql.SparkSession

object BooleanB {

  def main(args : Array[String]) : Unit = {

    val warehouse = System.getProperty("user.dir") + "\\spark-warehouse"

    val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.warehouse.dir", warehouse)
      .getOrCreate

    import spark.implicits._

    val C = spark.createDataset(Seq(
      (57, DenseVector(1, 0, 1, 1, 1)),
      (13, DenseVector(1, 0, 0, 0, 1)),
      (44, DenseVector(0, 0, 0, 0, 1)),
      (11, DenseVector(0, 1, 1, 1, 0))
    ))

    val S = spark.createDataset(Seq(
      (1, DenseVector(1.0, 0.8, 0.3, 0.5, 0.5)),
      (2, DenseVector(0.8, 1.0, 0.5, 0.3, 0.7)),
      (3, DenseVector(0.3, 0.5, 1.0, 0.1, 0.8)),
      (4, DenseVector(0.5, 0.3, 0.1, 1.0, 0.2)),
      (5, DenseVector(0.5, 0.7, 0.8, 0.2, 1.0))
    ))


//    C.rdd.cartesian(S.rdd).map {
//      case ((customerId, customerVector), (itemId, itemVector)) =>
//
//
//
//        customerVector.data.zip(itemVector.data)
//          .filter(_._1 == 1) // if customer bought this item
//          .map(_)
//    }

  }

}
