package com.github.projectflink.als

import breeze.linalg.{DenseMatrix, DenseVector}
import org.apache.flink.core.memory.{DataOutputView, DataInputView}
import org.apache.flink.types.Value

object ALSUtils {
  case class ALSDGConfig(numListeners: Int = 0, numSongs: Int = 0, numLatentVariables: Int = 0,
                         meanEntries: Double = 0,
                         varEntries: Double = 0,
                         meanNumRankingEntries: Double = 0,
                         varNumRankingEntries: Double = 0,
                         outputPath: String = null)

  case class ALSConfig(numListeners: Int = 0, numSongs: Int = 0, numLatentVariables: Int = 0,
                       lambda: Double = 0,
                       maxIterations: Int = 0,
                       inputRankings: String = null, outputPath: String = null)

  val RANKING_MATRIX = "rankingMatrix"
  val SONGS_MATRIX = "songsMatrix"
  val LISTENERS_MATRIX = "listenersMatrix"

  case class Vector(id: Long, entries: Array[Double]) {
    override def toString: String = {
      s"($id,[${entries.mkString(",")}])"
    }
  }

  case class BreezeVector(idx: Int, wrapper: BreezeDenseVectorWrapper)

  class BreezeDenseVectorWrapper(var vector: DenseVector[Double]) extends Value{
    def this() = this(null)

    override def toString = {
      vector.toString
    }

    override def write(out: DataOutputView): Unit = {
      out.writeInt(vector.length)
      vector.foreach{ out.writeDouble(_) }
    }

    override def read(in: DataInputView): Unit = {
      val length = in.readInt()
      val data = new Array[Double](length)

      for(i <- 0 until length){
        data(i) = in.readDouble()
      }

      vector = DenseVector(data)
    }
  }

  object BreezeDenseVectorWrapper{
    def apply(vector: DenseVector[Double]) = new BreezeDenseVectorWrapper(vector)
    def apply(data: Array[Double]) = new BreezeDenseVectorWrapper(DenseVector(data))
    def apply(data: Seq[Double]) = new BreezeDenseVectorWrapper(DenseVector(data :_*))

    def unapply(wrapper: BreezeDenseVectorWrapper): Option[DenseVector[Double]] = {
      Some(wrapper.vector)
    }

    implicit def unwrap(wrapper: BreezeDenseVectorWrapper): DenseVector[Double] = {
      wrapper.vector
    }
  }

  implicit def wrapBreezeDenseVector(vector: DenseVector[Double]): BreezeDenseVectorWrapper =
    BreezeDenseVectorWrapper(vector)

  implicit def wrapArray(array: Array[Double]): BreezeDenseVectorWrapper = {
    BreezeDenseVectorWrapper(array)
  }

  implicit def wrapSeq(seq: Seq[Double]): BreezeDenseVectorWrapper = {
    BreezeDenseVectorWrapper(seq)
  }

  case class BreezeMatrix(idx: Int, wrapper: BreezeDenseMatrixWrapper)

  class BreezeDenseMatrixWrapper(var matrix: DenseMatrix[Double]) extends Value{
    def this() = this(null)

    override def toString = {
      matrix.toString
    }

    override def write(out: DataOutputView): Unit = {
      out.writeInt(matrix.rows)
      out.writeInt(matrix.cols)

      for(value <- matrix.valuesIterator){
        out.writeDouble(value)
      }
    }

    override def read(in: DataInputView): Unit = {
      val rows = in.readInt()
      val cols = in.readInt()

      matrix = DenseMatrix.zeros[Double](rows, cols)

      for(r <- 0 until rows; c <- 0 until cols){
        matrix(r,c) = in.readDouble()
      }
    }
  }

  object BreezeDenseMatrixWrapper {

    def unapply(wrapper: BreezeDenseMatrixWrapper): Option[DenseMatrix[Double]] = {
      Some(wrapper.matrix)
    }

    implicit def wrapBreezeDenseMatrix(matrix: DenseMatrix[Double]) = {
      new BreezeDenseMatrixWrapper(matrix)
    }

    implicit def unwrap(wrapper: BreezeDenseMatrixWrapper): DenseMatrix[Double] = {
      wrapper.matrix
    }
  }

}
