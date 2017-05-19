package hr.fer.ztel.dipl.ml

//noinspection SimplifiableFoldOrReduce
object SparseVectorAlgebra extends Serializable {

  def addV(v1 : Map[Int, Double], v2 : Map[Int, Double]) : Map[Int, Double] = {
    (v1.keySet union v2.keySet)
      .map(k => (k, v1.getOrElse(k, 0.0) + v2.getOrElse(k, 0.0)))
      .withFilter(_._2 != 0.0)
      .map(t => t)
      .toMap
  }

  def addM(v1 : Map[(Int, Int), Double], v2 : Map[(Int, Int), Double]) : Map[(Int, Int), Double] = {
    (v1.keySet union v2.keySet)
      .map(k => (k, v1.getOrElse(k, 0.0) + v2.getOrElse(k, 0.0)))
      .withFilter(_._2 != 0.0)
      .map(t => t)
      .toMap
  }

  def dot(v1 : Map[Int, Double], v2 : Map[Int, Double]) : Double = {
    if (v1.size <= v2.size)
      v1.map { case (k, v) => v * v2.getOrElse(k, 0.0) }
        .withFilter(_ != 0.0)
        .map(v => v)
        .reduce(_ + _)
    else
      v2.map { case (k, v) => v * v1.getOrElse(k, 0.0) }
        .withFilter(_ != 0.0)
        .map(v => v)
        .reduce(_ + _)
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

  // izlaz je parcijalna matrica
  def outer(vector1 : Map[Int, Double], vector2 : Map[Int, Double]) : Map[(Int, Int), Double] = {
    /* v1 = 1 */    for {
      (k1, v1) <- vector1
      (k2, v2) <- vector2
      mul = v1 * v2
      if mul != 0.0
    } yield ((k1, k2), mul)
  }

  // izlaz je parcijalna matrica
  def outer(vector1 : Set[Int], vector2 : Map[Int, Double]) : Map[(Int, Int), Double] = {
    /* v1 = 1 */
    val set = for {
      k1 <- vector1 // v1 = 1
      (k2, v2) <- vector2
    } yield ((k1, k2), v2) // 1 * v2
    set.toMap
  }

}
