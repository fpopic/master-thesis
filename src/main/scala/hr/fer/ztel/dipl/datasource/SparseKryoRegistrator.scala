package hr.fer.ztel.dipl.datasource

import breeze.linalg.{SparseVector => BreezeSparseVector}
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

object SparseKryoRegistrator extends KryoRegistrator {

  override def registerClasses(kryo : Kryo) {
    kryo.register(classOf[Set[Int]]) // bool vector
    kryo.register(classOf[Map[Int, Double]]) // vector
    kryo.register(classOf[Map[(Int, Int), Double]]) // matrix

    //
    kryo.register(classOf[BreezeSparseVector[Double]])
    kryo.register(classOf[Array[Int]])
    kryo.register(classOf[Array[Double]])


  }

}



