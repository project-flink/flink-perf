package com.github.projectflink.spark.als

import com.github.projectflink.common.als.Factors
import org.apache.spark.{SparkContext, SparkConf}


import org.apache.spark.mllib.recommendation.{ALS => SparkALS, Rating => SparkRating}

object ALSMLLib extends ALSSparkRunner with ALSSparkToyRatings {

  def main(args: Array[String]): Unit = {
    parseCL(args) map {
      config => {
        import config._

        val sparkConf = new SparkConf().setMaster(master).setAppName("ALS")
        sparkConf.set("spark.hadoop.skipOutputChecks", "false")

        val sc = new SparkContext(sparkConf)

        val ratings = readRatings(inputRatings, sc) map { rating => SparkRating(rating.user,
          rating.item, rating.rating)}

        val model = SparkALS.train(ratings, factors, iterations, lambda, blocks)

        val userFactors = model.userFeatures map {
          case (id, factors) => new Factors(id, factors)
        }

        val itemFactors = model.userFeatures map {
          case (id, factors) => new Factors(id, factors)
        }

        val factorization = Factorization(userFactors, itemFactors)

        outputFactorization(factorization, outputPath)
      }
    } getOrElse{
      println("Could not parse command line arguments.")
    }
  }
}
