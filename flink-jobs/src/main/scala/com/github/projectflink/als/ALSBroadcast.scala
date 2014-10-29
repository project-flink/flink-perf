package com.github.projectflink.als

import breeze.linalg.{diag, DenseVector, DenseMatrix}
import com.github.projectflink.common.als.{Rating, Factors, outerProduct}
import org.apache.flink.api.common.functions.{RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import scala.collection.mutable
import org.apache.flink.api.scala._

class ALSBroadcast(factors: Int, lambda: Double, iterations: Int, seed: Long) extends
ALSFlinkAlgorithm {

  val BROADCAST_MATRIX = "broadcastMatrix"

  override def factorize(ratings: DS[RatingType]): Factorization = {
    val itemIDS = ratings map { rating => Tuple1(rating.item) } distinct

    val initialItemMatrix = generateRandomMatrix(itemIDS map { _._1 }, factors, seed)

    val updateMatrix = new RichMapFunction[Array[RatingType], FactorType]() {
      import scala.collection.JavaConverters._
      val matrix = mutable.HashMap[IDType, FactorType]()

      override def open(config: Configuration): Unit ={

        for(itemFactor <- getRuntimeContext.getBroadcastVariable[FactorType]
          (BROADCAST_MATRIX).asScala){
          matrix += (itemFactor.id -> itemFactor)
        }
      }

      override def map(ratings: Array[RatingType]): FactorType = {
        val n = ratings.length

        val v = DenseVector.zeros[Double](factors)
        val A = DenseMatrix.zeros[Double](factors, factors)

        for(Rating(_, id, rating) <- ratings){

          val factorVector = matrix(id)
          val vector = DenseVector(factorVector.factors)

          v += vector * rating

          import outerProduct._
          A += outerProduct(vector, vector)
        }

        diag(A) += lambda * n

        val targetID = ratings(0).user

        new Factors(targetID, (A \ v).data)
      }
    }

    val userRatings = ratings.groupBy(0).reduceGroup{
      (ratings, col:Collector[Array[RatingType]]) => {
        col.collect(ratings.toArray)
      }
    }

    val itemRatings = ratings.groupBy(1).reduceGroup{
      (ratings, col: Collector[Array[RatingType]]) => {
        val shuffeledRatings = ratings map { rating => new Rating(rating.item, rating.user,
          rating.rating)}
        col.collect(shuffeledRatings.toArray)
      }
    }

    val itemMatrix = initialItemMatrix.iterate(iterations){
      itemMatrix => {
        val u = userRatings.map(updateMatrix).withBroadcastSet(itemMatrix, BROADCAST_MATRIX)
        itemRatings.map(updateMatrix).withBroadcastSet(u, BROADCAST_MATRIX)
      }
    }

    val userMatrix = userRatings.map(updateMatrix).withBroadcastSet(itemMatrix, BROADCAST_MATRIX)

    Factorization(userMatrix, itemMatrix)
  }
}

object ALSBroadcast extends ALSFlinkRunner with ALSFlinkToyRatings {
  def main(args: Array[String]): Unit = {
    parseCL(args) map {
      config => {
        import config._

        val env = ExecutionEnvironment.getExecutionEnvironment
        val ratings = readRatings(inputRatings, env)

        val als = new ALSJoin(factors, lambda, iterations, seed)
        val factorization = als.factorize(ratings)

        outputFactorization(factorization, outputPath)

        env.execute("ALS benchmark")
      }
    } getOrElse{
      println("Could not parse command line parameters.")
    }
  }
}


