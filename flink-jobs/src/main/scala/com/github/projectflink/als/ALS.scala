package com.github.projectflink.als

import org.jblas.FloatMatrix

object ALS {
  def outerProduct(vector: FloatMatrix, matrix: FloatMatrix, factors: Int): Unit = {
    val vd =  vector.data
    val md = matrix.data

    var row = 0
    var pos = 0
    while(row < factors){
      var col = 0
      while(col <= row){
        md(pos) = vd(row) * vd(col)
        col += 1
        pos += 1
      }

      row += 1
    }
  }

  def generateFullMatrix(triangularMatrix: FloatMatrix, fmatrix: FloatMatrix, factors: Int): Unit
  = {
    var row = 0
    var pos = 0
    val fmd = fmatrix.data
    val tmd = triangularMatrix.data

    while(row < factors){
      var col = 0
      while(col < row){
        fmd(row*factors + col) = tmd(pos)
        fmd(col*factors + row) = tmd(pos)

        pos += 1
        col += 1
      }

      fmd(row*factors + row) = tmd(pos)

      pos += 1
      row += 1
    }
  }
}
