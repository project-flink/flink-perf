package com.github.projectflink.spark.als

import com.github.projectflink.common.als.{ALSToyRatings}

trait ALSSparkToyRatings extends ALSSpark with ALSToyRatings {
  self: ALSSparkRunner =>

  abstract override def readRatings(input: String, ctx: Context): DS[RatingType] = {
    ctx.parallelize(toyRatings)
  }
}
