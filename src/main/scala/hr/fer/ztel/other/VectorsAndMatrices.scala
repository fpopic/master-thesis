package hr.fer.ztel.other

object VectorsAndMatrices {

  def main(args : Array[String]) : Unit = {

    import breeze.linalg._

    val items = Seq((0, 1.0), (1, 5.0), (4, 7.0))

    val v1 : SparseVector[Double] = SparseVector(5)(items : _*)
    val v2 : SparseVector[Double] = SparseVector(5)(items : _*)

    val v3 = v1.dot(v2)
    // v1 * v2 is elementwise
    println(v3)

    v3.

    val x = 174545221
    //    scaleIdLongToIndexInt(x)
    //    def scaleIdLongToIndexInt(id : Long)(a : Int = 0, b : Int = Int.MaxValue) : Int = (a + id % (b - a + 1)).toInt

    def add(v1 : Map[Int, Double], v2 : Map[Int, Double]) : Map[Int, Double] = {
      (v1.keySet union v2.keySet)
        .map(k => (k, v1.getOrElse(k, 0.0) + v2.getOrElse(k, 0.0)))
        .toMap
    }

    val a : Map[Int, Double] = Map()
    val b : Map[Int, Double] = Map()

    val c : Map[Int, Double] = add(a, b)

    println(s"c = ${c}")


  }
}
