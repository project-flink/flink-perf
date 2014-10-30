package com.github.projectflink.als

import breeze.linalg.{DenseMatrix, diag, DenseVector}
import com.github.projectflink.common.als.{outerProduct, Factors, Rating}
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector


class ALSJoin(factors: Int, lambda: Double,
              iterations: Int, seed: Long) extends ALSFlinkAlgorithm with
Serializable {

  def factorize(ratings: DS[RatingType]): Factorization = {
    null
  }

  def factorize(ratings: DS[RatingType], ratings2: DS[RatingType],
                ratings3: DS[RatingType]): Factorization = {
    val transposedRatings = ratings2 map { x => Rating(x.item, x.user, x.rating)}

    val itemIDs = ratings map { x => Tuple1(x.item) } distinct

    val initialItemMatrix = generateRandomMatrix(itemIDs map { _._1 }, factors, seed)

    val itemMatrix = initialItemMatrix.iterate(iterations){
      itemMatrix => {
        val userMatrix = updateMatrix(ratings, itemMatrix, lambda)
        updateMatrix(transposedRatings, userMatrix, lambda)
      }
    }

    val userMatrix = updateMatrix(ratings3, itemMatrix, lambda)

    Factorization(userMatrix, itemMatrix)
  }

  def updateMatrix(ratings: DataSet[RatingType], items: DataSet[FactorType],
                   lambda: Double): DataSet[FactorType] = {
    val uVA = items.join(ratings).where(0).equalTo(1) {
      (item, ratingEntry) => {
        val Rating(uID, _, rating) = ratingEntry

        (uID, rating, item.factors)
      }
    }

    uVA.groupBy(0).reduceGroup{
      (vectors, col: Collector[FactorType]) => {
        import outerProduct._

        var uID = -1
        var matrix = DenseMatrix.zeros[Double](factors, factors)
        var vector = DenseVector.zeros[Double](factors)
        var n = 0

        for((id, rating, vectorData) <- vectors){
          uID = id
          val v = DenseVector(vectorData)

          vector += v * rating
          matrix += outerProduct(v, v)

          n += 1
        }

        diag(matrix) += n*lambda

        col.collect(new Factors(uID, (matrix \ vector).data))
      }
    }
  }
}

object ALSJoin extends ALSFlinkRunner with ALSFlinkToyRatings {
  def main(args: Array[String]): Unit = {
    parseCL(args) map {
      config => {
        import config._

        val env = ExecutionEnvironment.getExecutionEnvironment
        val ratings = readRatings(inputRatings, env)
        val ratings2 = readRatings(inputRatings, env)
        val ratings3 = readRatings(inputRatings, env)

        val als = new ALSJoin(factors, lambda, iterations, seed)
        val factorization = als.factorize(ratings, ratings2, ratings3)

        outputFactorization(factorization, outputPath)

        env.execute("ALS benchmark")
      }
    } getOrElse{
      println("Could not parse command line parameters.")
    }
  }
}
