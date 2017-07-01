package hr.fer.ztel.excluded

import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession

object ArgTopTester {

  def main(args: Array[String]): Unit = {

    import breeze.linalg.argtopk
    import org.apache.spark.mllib.linalg.MLlibBreezeConversions._

    val spark = SparkSession.builder.master("local[*]").getOrCreate()

    val a = Vectors.dense(1, 44, 9, 100, 34)
    val b = argtopk(a.toBreeze, 4)

    println(s"b = ${b}")
  }

}
