package hr.fer.ztel.thesis.spark

import breeze.linalg.{SparseVector => BreezeSparseVector}
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

object SparseKryoRegistrator extends KryoRegistrator {

  override def registerClasses(kryo : Kryo) {
    // my sparse linear algebra
    kryo.register(classOf[Array[Int]]) // bool vector
    kryo.register(classOf[Map[Int, Double]]) // vector
    kryo.register(classOf[Map[(Int, Int), Double]]) // matrix

    // breeze sparse linear algebra
    kryo.register(classOf[BreezeSparseVector[Double]])
    kryo.register(classOf[Array[Double]])
  }

}