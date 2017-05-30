package hr.fer.ztel.thesis.ml

//noinspection SimplifiableFoldOrReduce
object SparseLinearAlgebra extends Serializable {

  def normalize(v : Map[Int, Double]) : Map[Int, Double] = {
    val norm = v.reduce((a, b) => (a._1, a._2 + b._2))._2
    if (norm != 0.0)
      v.map { case (key, value) => (key, value / norm) }
    else
      v
  }

  def dot(v1 : Array[Int], v2 : Map[Int, Double]) : Double = {
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

  def outer(vector1 : Array[Int], vector2 : Map[Int, Double]) : Map[(Int, Int), Double] = {
    /* v1 = 1 */
    val array = for {
      k1 <- vector1 // v1 = 1
      (k2, v2) <- vector2
    } yield ((k1, k2), v2) // 1 * v2
    array.toMap
  }

}
