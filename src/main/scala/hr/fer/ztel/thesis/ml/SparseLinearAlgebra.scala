package hr.fer.ztel.thesis.ml

//noinspection SimplifiableFoldOrReduce
object SparseLinearAlgebra extends Serializable {

  def dot(vector1 : Array[Int], vector2 : Map[Int, Double]) : Double = {
    if (vector1.length <= vector2.size)
      vector1.map(vector2.getOrElse(_, 0.0))
        .withFilter(_ != 0.0)
        .map(v => v)
        .reduce(_ + _)
    else
      vector2.map { case (k, v) => if (vector1 contains k) v else 0 }
        .withFilter(_ != 0.0)
        .map(v => v)
        .reduce(_ + _)
  }

  def outer(vector1 : Array[Int], vector2 : Map[Int, Double]) : Map[(Int, Int), Double] = {
    /* v1 = 1 because bool vector*/
    val array = for {
      k1 <- vector1 // v1 <- 1
      (k2, v2) <- vector2
    } yield ((k1, k2), v2) // ((k1,k2), 1 * v2)
    array.toMap
  }

}