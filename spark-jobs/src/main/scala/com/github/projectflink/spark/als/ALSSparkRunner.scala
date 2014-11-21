package com.github.projectflink.spark.als

import com.github.projectflink.common.als.{Rating, ALSRunner}
import org.apache.spark.SparkContext

import scala.reflect.io.Path

trait ALSSparkRunner extends ALSSpark with ALSRunner {
  type Context = SparkContext

  def readRatings(input: String, ctx: Context): DS[RatingType] = {
    ctx.textFile(input) map {
      line => {
        val splits = line.split(",")
        Rating[IDType, ElementType](splits(0).toInt, splits(1).toInt, splits(2).toDouble
          .asInstanceOf[ElementType])
      }
    }
  }

  def outputFactorization(factorization: ALSSpark#Factorization, outputPath: String): Unit = {
    if(outputPath == null){
      factorization.userFactors.foreach(println _)
      factorization.itemFactors.foreach(println _)
    }else{
      val path = if(outputPath.endsWith("/")) outputPath else outputPath + "/"
      val userFactorsPath = path + USER_FACTORS_FILE
      val itemFactorsPath = path + ITEM_FACTORS_FILE

      factorization.userFactors.saveAsTextFile(userFactorsPath)
      factorization.itemFactors.saveAsTextFile(itemFactorsPath)
    }
  }

}
