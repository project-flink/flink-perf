package com.github.projectflink.spark.als

import breeze.linalg.{diag, DenseVector}
import com.github.projectflink.common.als.{Factors, outerProduct, Rating}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

class ALSJoin(factors: Int, lambda: Double, iterations: Int,
              seed: Long) extends ALSSparkAlgorithm with Serializable{

  def factorize(ratings: DS[RatingType]): Factorization = {
    val adjacencies = ratings map { case Rating(listener, song, _) => (listener, song) }

    val itemIDs = ratings.map{_.item} distinct()

    var itemMatrix = generateRandomMatrix(itemIDs, factors, seed) map { item => (item.id,
      item.factors)}

    val uRankings = ratings map {
      case Rating(listener, song, ranking) => (song, (listener, ranking))
    }

    val mRankings = ratings map {
      case Rating(listener, song, ranking) => (listener, (song, ranking))
    }

    val uAdjacencies = adjacencies map {
      case (listener, song) => (song, listener)
    }

    var userMatrix: DS[(IDType, Array[ElementType])] = null

    uRankings.persist()
    mRankings.persist()
    uAdjacencies.persist()
    adjacencies.persist()

    for(i <- 1 to iterations){
      userMatrix = updateMatrix(uRankings, itemMatrix, uAdjacencies, lambda)
      itemMatrix = updateMatrix(mRankings, userMatrix, adjacencies, lambda)
    }

    userMatrix = updateMatrix(uRankings, itemMatrix, uAdjacencies, lambda)

    Factorization(userMatrix map {case (id, factors) => new Factors(id, factors)},
      itemMatrix map { case (id, factors) => new Factors(id, factors)})
  }


  def updateMatrix(ratings: DS[(IDType,(IDType, ElementType))], matrix:DS[(IDType,
    Array[ElementType])], adjacencies: DS[(IDType, IDType)], lambda: Double):DS[(IDType,
    Array[ElementType])] = {

    val uV = ratings.join(matrix).map {
      case (_, ((listener, ranking), factorArray)) =>
        (listener, (DenseVector(factorArray) * ranking):DenseVector[Double])
    }.reduceByKey(_ + _)

    val uA = adjacencies.join(matrix).map {
      case (_, (listener, factorVector)) =>{
        val vector = DenseVector(factorVector)
        import outerProduct._
        val partialA = outerProduct(vector, vector)
        diag(partialA) += lambda
        (listener, partialA)
      }
    }.reduceByKey(_ + _)

    uA.join(uV).map{
      case (listener, (matrix, vector)) =>
        (listener, (matrix \ vector).data)
    }
  }
}

object ALSJoin extends ALSSparkRunner with ALSSparkToyRatings {
  def main(args: Array[String]): Unit = {
    parseCL(args) map {
      config => {
        import config._

        val conf = new SparkConf().setMaster(master).setAppName("ALS")
        conf.set("spark.hadoop.skipOutputChecks", "false")

        val sc = new SparkContext(conf)

        val ratings = readRatings(inputRatings, sc)

        val als = new ALSJoin(factors, lambda, iterations, seed)

        val factorization = als.factorize(ratings)

        outputFactorization(factorization, outputPath)
      }
    } getOrElse{
      println("Error parsing the command line arguments.")
    }
  }
}
