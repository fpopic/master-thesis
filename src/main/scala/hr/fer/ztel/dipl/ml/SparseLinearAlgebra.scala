package hr.fer.ztel.dipl.ml

//noinspection SimplifiableFoldOrReduce
object SparseLinearAlgebra extends Serializable {

  // inner

  def addV(v1 : Map[Int, Double], v2 : Map[Int, Double]) : Map[Int, Double] = {
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

  // outer

  def addM(m1 : Map[(Int, Int), Double], m2 : Map[(Int, Int), Double]) : Map[(Int, Int), Double] = {
    (m1.keySet union m2.keySet)
      .map(k => (k, m1.getOrElse(k, 0.0) + m2.getOrElse(k, 0.0)))
      .withFilter(_._2 != 0.0)
      .map(t => t)
      .toMap
  }

  def outer(v1 : Map[Int, Double], v2 : Map[Int, Double]) : Map[(Int, Int), Double] = {
    // izlaz je parcijalna matrica
    /* v1 = 1 */
    for {
      (key1, value1) <- v1
      (key2, value2) <- v2
      mul = value1 * value2
      if mul != 0.0
    } yield ((key1, key2), mul)
  }

  ////////////////////////////////// bool values //////////////////////////////////

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

  def outer(vector1 : Set[Int], vector2 : Map[Int, Double]) : Map[(Int, Int), Double] = {
    // izlaz je parcijalna matrica
    /* v1 = 1 */
    val set = for {
      k1 <- vector1 // v1 = 1
      (k2, v2) <- vector2
    } yield ((k1, k2), v2) // 1 * v2
    set.toMap
  }

}
