package com.github.projectflink.als

import com.github.projectflink.common.als.{ALSToyRatings}
import org.apache.flink.api.scala._

trait ALSFlinkToyRatings extends ALSToyRatings {
  self: ALSFlinkRunner =>

  abstract override def readRatings(input: String, ctx: Context): DS[RatingType] = {
    ctx.fromCollection(toyRatings)
  }
}
