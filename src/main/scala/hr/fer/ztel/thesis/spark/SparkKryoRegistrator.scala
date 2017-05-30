package hr.fer.ztel.thesis.spark

import breeze.linalg.{SparseVector => BreezeSparseVector, DenseVector=>BreezeDenseVector}
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

class SparkKryoRegistrator extends KryoRegistrator {

  override def registerClasses(kryo : Kryo) {
    kryo.register(classOf[Array[Int]]) // bool vector
    kryo.register(classOf[Map[Int, Double]]) // vector
    kryo.register(classOf[Map[(Int, Int), Double]]) // matrix
    kryo.register(classOf[BreezeDenseVector[Double]])
    kryo.register(classOf[BreezeSparseVector[Double]])
    kryo.register(classOf[Array[Double]])
  }

}