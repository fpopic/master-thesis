package hr.fer.ztel.thesis.sparse_linalg

//noinspection SimplifiableFoldOrReduce
object SparseVectorOperators extends Serializable {

  def dot(v1: Array[Int], v2: Map[Int, Double]): Double = {
    if (v1.length <= v2.size)
      v1.map(v2.getOrElse(_, 0.0))
        .withFilter(_ != 0.0)
        .map(v => v)
        .reduce(_ + _)
    else
      v2.map { case (k, v) => if (v1 contains k) v else 0 }
        .withFilter(_ != 0.0)
        .map(v => v)
        .reduce(_ + _)
  }

  def outer(vector1: Array[Int], vector2: Map[Int, Double]): Map[(Int, Int), Double] = {
    /* v1 = 1 because bool vector*/
    val array = for {
      k1 <- vector1 // v1 <- 1
      (k2, v2) <- vector2
      if v2 != 0
    } yield ((k1, k2), v2) // ((k1,k2), 1 * v2)
    array.toMap
  }

  def outer(vector1: Set[Int], vector2: Map[Int, Double]): Map[(Int, Int), Double] = {
    /* v1 = 1 because bool vector*/
    val array = for {
      k1 <- vector1 // v1 <- 1
      (k2, v2) <- vector2
      if v2 != 0
    } yield ((k1, k2), v2) // ((k1,k2), 1 * v2)
    array.toMap
  }

  def normalize(v: Array[(Int, Double)]): Map[Int, Double] = {
    val norm = v.foldLeft(0.0)((sum, t) => sum + t._2)
    if (norm != 0)
      v.map { case (item, similarity) => (item, similarity / norm) }.toMap
    else
      v.toMap
  }

  def argTopK(v: Array[(Int, Double)], k: Int): Array[Int] = v.sortWith(_._2 > _._2).take(k).map(_._1)

}