package com.github.projectflink.als

import com.github.projectflink.als.ALSUtils.{BreezeVector, ALSDGConfig}
import org.apache.flink.api.scala.DataSet

trait ALSBase {
  val defaultDataGenerationConfig = ALSDGConfig(10, 15, 5, 10, 1, 4, 1)
  val SCALING_FACTOR = 100
  val U_MATRIX_FILE = "uMatrix"
  val M_MATRIX_FILE = "mMatrix"

  type ColMatrix = DataSet[BreezeVector]
  type CellMatrix = DataSet[(Int, Int, Double)]
  type AdjacencyMatrix = DataSet[(Int, Int)]
}
