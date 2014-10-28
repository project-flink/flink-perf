package com.github.projectflink.spark.als

import com.github.projectflink.common.als.{ALSRunner, ALSToyRatings}

trait ALSSparkToyRatings extends ALSSpark with ALSRunner with ALSToyRatings {
  that: ALSSparkRunner =>

  abstract override def readRatings(input: String, ctx: Context): DS[RatingType] = {
    if(input != null && input.nonEmpty){
      super.readRatings(input, ctx)
    }else {
      ctx.parallelize(toyRatings)
    }
  }
}
