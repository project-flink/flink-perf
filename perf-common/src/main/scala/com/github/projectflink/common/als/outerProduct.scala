package com.github.projectflink.common.als


import breeze.generic.UFunc
import breeze.linalg.operators.OpMulMatrix
import breeze.linalg.{DenseMatrix, DenseVector}
import breeze.storage.Zero

import scala.reflect.ClassTag

object outerProduct extends UFunc{

  implicit def outerProductDVDV[T](implicit classTag: ClassTag[T],
                                                   zero: Zero[T], op: OpMulMatrix
  .Impl2[DenseVector[T], T,
    DenseVector[T]]) = new Impl2[DenseVector[T], DenseVector[T], DenseMatrix[T]] {
    override def apply(v: DenseVector[T], v2: DenseVector[T]): DenseMatrix[T] = {
      val result = DenseMatrix.zeros[T](v.length, v2.length)

      for((col, value) <- v2.activeIterator){
        result(::, col) := v * value
      }

      result
    }
  }
}
