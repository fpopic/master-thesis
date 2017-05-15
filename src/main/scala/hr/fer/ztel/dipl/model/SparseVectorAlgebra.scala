package hr.fer.ztel.dipl.model

//noinspection SimplifiableFoldOrReduce
object SparseVectorAlgebra extends Serializable {

  def add(v1 : Map[Int, Double], v2 : Map[Int, Double]) : Map[Int, Double] = {
    (v1.keySet union v2.keySet)
      .map(k => (k, v1.getOrElse(k, 0.0) + v2.getOrElse(k, 0.0)))
      .toMap
  }

  def dot(v1 : Map[Int, Double], v2 : Map[Int, Double]) : Double = {
    if (v1.size <= v2.size)
      v1.map { case (k, v) => v * v2.getOrElse(k, 0.0) }.reduce(_ + _)
    else
      v2.map { case (k, v) => v * v1.getOrElse(k, 0.0) }.reduce(_ + _)
  }

  /**
    *
    * @param v1 binary sparse vector, same as ''Map[Int, Double]'' with all values equall to 1.0
    * @param v2 sparse vector with integer indices and double values
    *
    * @return
    */
  def dot(v1 : Set[Int], v2 : Map[Int, Double]) : Double = {
    if (v1.size <= v2.size)
      v1.map(v2.getOrElse(_, 0.0)).reduce(_ + _)
    else
      v2.map { case (k, v) => if (v1 contains k) v else 0 }.reduce(_ + _)
  }

  def outer(vector1 : Map[Int, Double], vector2 : Map[Int, Double]) : Map[(Int, Int), Double] = {
    // same as flatMap (not for loop)
    for {
      (k1, v1) <- vector1
      (k2, v2) <- vector2
    } yield ((k1, k2), v1 * v2)
  }

  def outer(vector1 : Set[Int], vector2 : Map[Int, Double]) : Map[(Int, Int), Double] = {
    // same as flatMap (not for loop)
    vector1.flatMap {
      case (k1) =>
        vector2.map { case (k2, v2) => ((k1, k2), v1 * v2) }
    }
  }

}
