package com.github.projectflink.als

import com.github.projectflink.common.als.{ALSUtils, Rating, Factors}
import com.github.projectflink.util.FlinkTools
import org.apache.flink.api.common.functions.{RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import org.jblas.{Solve, SimpleBlas, FloatMatrix}
import scala.collection.mutable
import org.apache.flink.api.scala._

import scala.collection.mutable.ArrayBuffer

class ALSBroadcast(factors: Int, lambda: Double, iterations: Int, seed: Long, persistencePath:
Option[String]) extends
ALSFlinkAlgorithm with Serializable{

  val BROADCAST_MATRIX = "broadcastMatrix"

  override def factorize(ratings: DS[RatingType]): Factorization = {

    val updateMatrix = new RichMapFunction[(IDType, Array[(IDType, ElementType)]), FactorType]() {
      import scala.collection.JavaConverters._
      val matrix = mutable.HashMap[IDType, FactorType]()
      val triangleSize = (factors* factors - factors)/2 + factors
      val xtx = FloatMatrix.zeros(triangleSize)
      val fullMatrix = FloatMatrix.zeros(factors, factors)
      val vector = FloatMatrix.zeros(factors)

      override def open(config: Configuration): Unit ={

        for(itemFactor <- getRuntimeContext.getBroadcastVariable[FactorType]
          (BROADCAST_MATRIX).asScala){
          matrix += (itemFactor.id -> itemFactor)
        }
      }

      override def map(ratings: (IDType, Array[(IDType, ElementType)])): FactorType = {
        val n = ratings._2.length
        xtx.fill(0.0f)
        vector.fill(0.0f)

        for((id, rating) <- ratings._2){
          val v = new FloatMatrix(matrix(id).factors)
          ALSUtils.outerProductInPlace(v, xtx, factors)
          SimpleBlas.axpy(rating, v, vector)
        }

        ALSUtils.generateFullMatrix(xtx, fullMatrix, factors)

        var counter = 0

        while(counter < factors){
          fullMatrix.data(counter * factors + counter) += lambda.asInstanceOf[ElementType] * n
          counter += 1
        }

        val targetID = ratings._1

        new Factors(targetID, Solve.solvePositive(fullMatrix, vector).data)
      }
    }

    val (initialItemMatrix, userRatings, itemRatings) = {
      val itemIDS = ratings map { rating => Tuple1(rating.item) } distinct

      val iiMatrix = generateRandomMatrix(itemIDS map { _._1 }, factors, seed)

      val uRatings = ratings.groupBy(0).reduceGroup{
        (ratings, col:Collector[(IDType, Array[(IDType, ElementType)])]) => {
          val buffer = new ArrayBuffer[(IDType, ElementType)]()
          var userID = 0

          while(ratings.hasNext){
            val Rating(user, item, rating) = ratings.next()
            buffer.+=((item, rating))
            userID = user
          }

          col.collect((userID, buffer.toArray))
        }
      }

      val iRatings = ratings.groupBy(1).reduceGroup{
        (ratings, col: Collector[(IDType, Array[(IDType, ElementType)])]) => {
          val buffer = new ArrayBuffer[(IDType, ElementType)]
          var itemID = 0

          while(ratings.hasNext){
            val Rating(user, item, rating) = ratings.next()
            buffer.+=((user, rating))
            itemID = item
          }

          col.collect((itemID, buffer.toArray))
        }
      }

      persistencePath match {
        case Some(p) => {
          val iiMatrixPath = p + "initialItemMatrix"
          val uRatingsPath = p + "userRatings"
          val iRatingsPath = p+ "itemRatings"
          FlinkTools.persist(iiMatrix, uRatings, iRatings, iiMatrixPath, uRatingsPath, iRatingsPath)
        }
        case None => (iiMatrix, uRatings, iRatings)
      }
    }

    val itemMatrix = initialItemMatrix.iterate(iterations){
      itemMatrix => {
        val u = userRatings.map(updateMatrix).withBroadcastSet(itemMatrix, BROADCAST_MATRIX)
        itemRatings.map(updateMatrix).withBroadcastSet(u, BROADCAST_MATRIX)
      }
    }

    val pItemMatrix = {
      persistencePath match {
        case Some(p) => {
          val itemMatrixPath = p + "itemMatrix"
          FlinkTools.persist(itemMatrix, itemMatrixPath)
        }
        case None => itemMatrix
      }
    }

    val userMatrix = userRatings.map(updateMatrix).withBroadcastSet(pItemMatrix, BROADCAST_MATRIX)

    Factorization(userMatrix, pItemMatrix)
  }
}

object ALSBroadcast extends ALSFlinkRunner with ALSFlinkToyRatings {
  def main(args: Array[String]): Unit = {
    parseCL(args) map {
      config => {
        import config._

        val env = ExecutionEnvironment.getExecutionEnvironment
        val ratings = readRatings(inputRatings, env)

        val als = new ALSBroadcast(factors, lambda, iterations, seed, persistencePath)
        val factorization = als.factorize(ratings)

        outputFactorization(factorization, outputPath)

        env.execute("ALS broadcast benchmark")
      }
    } getOrElse{
      println("Could not parse command line parameters.")
    }
  }
}


