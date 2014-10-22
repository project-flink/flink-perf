package com.github.projectflink.als

import breeze.generic.UFunc
import breeze.linalg._

object outerProduct extends UFunc{

  implicit object outerProductDVDV extends Impl2[DenseVector[Double], DenseVector[Double],
    DenseMatrix[Double]] {
    override def apply(v: DenseVector[Double], v2: DenseVector[Double]): DenseMatrix[Double] = {
      val result = DenseMatrix.zeros[Double](v.length, v2.length)

      for((col, value) <- v2.activeIterator){
        result(::, col) := v * value
      }

      result
    }
  }
}
