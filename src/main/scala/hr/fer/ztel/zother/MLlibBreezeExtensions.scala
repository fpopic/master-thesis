package org.apache.spark.mllib.linalg

import org.apache.spark.mllib.linalg.{Vector => MLlibVector}
import breeze.linalg.{Vector => BVector}

object MLlibBreezeExtensions {

  implicit class MLlibVectorPublications(val vector : MLlibVector) extends AnyVal {
    def toBreeze : BVector[scala.Double] = vector.asBreeze
  }

  implicit class BreezeVectorPublications(val breezeVector : BVector[Double]) extends AnyVal {
    def toMLlib : MLlibVector = Vectors.fromBreeze(breezeVector)
  }

}

