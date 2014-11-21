package com.github.projectflink.als

import com.github.projectflink.common.als.{ALSRunner, ALSToyRatings}
import org.apache.flink.api.scala._

trait ALSFlinkToyRatings extends ALSFlink with ALSRunner with ALSToyRatings {
  that: ALSFlinkRunner =>

  abstract override def readRatings(input: String, ctx: Context): DS[RatingType] = {
    if(input != null && input.nonEmpty){
      super.readRatings(input, ctx)
    }else {
      ctx.fromCollection(toyRatings).rebalance()
    }
  }
}
