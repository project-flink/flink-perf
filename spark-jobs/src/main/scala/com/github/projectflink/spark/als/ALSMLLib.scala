package com.github.projectflink.spark.als

import com.esotericsoftware.kryo.Kryo
import com.github.projectflink.common.als.Factors
import org.apache.spark.serializer.{KryoSerializer, KryoRegistrator}
import org.apache.spark.{SparkContext, SparkConf}


import org.apache.spark.mllib.recommendation.{ALS => SparkALS, Rating => SparkRating}

import scala.collection.mutable

object ALSMLLib extends ALSSparkRunner with ALSSparkToyRatings {

  class ALSRegistrator extends KryoRegistrator {
    override def registerClasses(kryo: Kryo) {
      kryo.register(classOf[SparkRating])
      kryo.register(classOf[mutable.HashSet[_]])
      kryo.register(classOf[mutable.BitSet])
    }
  }

  def main(args: Array[String]): Unit = {
    parseCL(args) map {
      config => {
        import config._

        val sparkConf = new SparkConf().setMaster(master).setAppName("ALS")
        sparkConf.set("spark.hadoop.skipOutputChecks", "false")
          .set("spark.hadoop.validateOutputSpecs", "false")
          .set("spark.serializer", classOf[KryoSerializer].getName)
          .set("spark.kryo.registrator", classOf[ALSRegistrator].getName)
          .set("spark.kryoserializer.buffer.mb", "10")

        val sc = new SparkContext(sparkConf)

        val ratings = readRatings(inputRatings, sc) map { rating => SparkRating(rating.user,
          rating.item, rating.rating)}

        val model = SparkALS.train(ratings, factors, iterations, lambda, blocks)

        val userFactors = model.userFeatures map {
          case (id, factors) => new Factors(id, factors)
        }

        val itemFactors = model.productFeatures map {
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
