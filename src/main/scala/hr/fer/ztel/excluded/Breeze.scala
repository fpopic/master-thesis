package hr.fer.ztel.excluded

import breeze.linalg.{DenseVector => BreezeDenseVector}
import hr.fer.ztel.thesis.spark.SparkSessionHandler
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession

object Breeze {

  def main(args: Array[String]): Unit = {
/*
    val handler = new SparkSessionHandler(args)
    implicit val spark: SparkSession = handler.getSparkSession

    import org.apache.spark.mllib.linalg.MLlibBreezeConversions._

    import breeze.numerics._
    import breeze.linalg._

    val mllibV: Vector[Double] = Vectors.dense(1.0, 2.0, 3.0).toBreeze
    val breezeV: BreezeDenseVector[Double] = BreezeDenseVector(1.0, 2.0, 3.0)


    val out = breezeV :* mllibV.toBreeze.t
*/


  }

}

