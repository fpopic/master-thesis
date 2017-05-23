package hr.fer.ztel.zother

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class SequenceRDD[T : ClassTag](
  sc : SparkContext,
  numPartitions : Int,
  rddLength : Int,
  createItem : Int => T
) extends RDD[T](sc, Nil) {

  @DeveloperApi
  override def compute(partition : Partition, context : TaskContext) : Iterator[T] = {

    val fullPartitionsLength = if (numPartitions == 1) rddLength else rddLength / (numPartitions - 1)

    val leftOverPartitionsLength = if (numPartitions == 1) 0 else rddLength % (numPartitions - 1)

    val isFullPartition = if (numPartitions == 1) true else partition.index != numPartitions - 1

    val length = if (isFullPartition) fullPartitionsLength else leftOverPartitionsLength

    Stream.from(partition.index * fullPartitionsLength).take(length).map(createItem).iterator
  }

  override protected def getPartitions() : Array[Partition] = {
    (0 until numPartitions).map(p => new DummyPartition(p)).toArray
  }

}

class DummyPartition(val partition : Int) extends Partition with Serializable {
  override val index : Int = partition
}

