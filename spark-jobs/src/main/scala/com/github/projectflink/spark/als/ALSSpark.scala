package com.github.projectflink.spark.als

import com.github.projectflink.common.als.ALS
import org.apache.spark.rdd.RDD

trait ALSSpark extends ALS {
  type DS[T] = RDD[T]
  type ElementType = Float
  type IDType = Int
}
