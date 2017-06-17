package hr.fer.ztel.thesis.ml

//noinspection SimplifiableFoldOrReduce
object SparseVectorOperators extends Serializable {

  def dot(a: Array[Int], b: Map[Int, Double]): Double = {
    if (a.length <= b.size)
      a.map(b.getOrElse(_, 0.0))
        .withFilter(_ != 0.0)
        .map(v => v)
        .reduce(_ + _)
    else
      b.map { case (k, v) => if (a contains k) v else 0 }
        .withFilter(_ != 0.0)
        .map(v => v)
        .reduce(_ + _)
  }

  def dot(a: Map[Int, Double], b: Map[Int, Double]): Double = {
    lazy val mul =
      if (a.size <= b.size)
        a.map { case (k1, v1) => v1 * b.getOrElse(k1, 0.0) }
      else
        b.map { case (k2, v2) => v2 * a.getOrElse(k2, 0.0) }

    mul.withFilter(_ != 0.0).map(v => v).reduce(_ + _)
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

  def outer(a: Map[Int, Double], b: Map[Int, Double]): Map[(Int, Int), Double] = {
    for {
      (k1, v1) <- a
      (k2, v2) <- b
      mul = v1 * v2
      if mul != 0
    } yield ((k1, k2), mul)
  }

  def normalize(a: Array[(Int, Double)]): Map[Int, Double] = {
    val norm = a.foldLeft(0.0)((sum, t) => sum + t._2)
    if (norm != 0)
      a.map { case (item, similarity) => (item, similarity / norm) }.toMap
    else
      a.toMap
  }
}