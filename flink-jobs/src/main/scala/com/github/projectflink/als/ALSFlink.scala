package com.github.projectflink.als

import com.github.projectflink.common.als.ALS
import org.apache.flink.api.scala.{DataSet}

trait ALSFlink extends ALS{
  type DS[T] = DataSet[T]
  type ElementType = Double
  type IDType = Int
}
