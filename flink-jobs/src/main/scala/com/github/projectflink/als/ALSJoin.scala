package com.github.projectflink.als

import breeze.linalg.{DenseMatrix, diag, DenseVector}
import com.github.projectflink.common.als.{outerProduct, Factors, Rating}
import org.apache.flink.api.scala._


class ALSJoin(factors: Int, lambda: Double,
              iterations: Int, seed: Long) extends ALSFlinkAlgorithm with
Serializable {

  def factorize(ratings: DS[RatingType]): Factorization = {
    val transposedRatings = ratings map { x => Rating(x.item, x.user, x.rating)}

    val itemIDs = ratings map { x => Tuple1(x.item) } distinct

    val initialItemMatrix = generateRandomMatrix(itemIDs map { _._1 }, factors, seed)

    val itemMatrix = initialItemMatrix.iterate(iterations){
      itemMatrix => {
        val userMatrix = updateMatrix(ratings, itemMatrix, lambda)
        updateMatrix(transposedRatings, userMatrix, lambda)
      }
    }

    val userMatrix = updateMatrix(ratings, itemMatrix, lambda)

    Factorization(userMatrix, itemMatrix)
  }

  def updateMatrix(ratings: DataSet[RatingType], items: DataSet[FactorType],
                   lambda: Double): DataSet[FactorType] = {
    val uVA = items.join(ratings).where(0).equalTo(1) {
      (item, ratingEntry) => {
        val Rating(uID, _, rating) = ratingEntry
        val vector: DenseVector[ElementType] = DenseVector(item.factors)
        val ratedVector: DenseVector[ElementType] = vector * rating

        import outerProduct._
        val partialMatrix = outerProduct(vector, vector)

        diag(partialMatrix) += lambda

        (new Factors(uID, ratedVector.data), new Factors(uID, partialMatrix.data))
      }
    }

    val uV = uVA.map(_._1).groupBy(0).reduce {
      (left, right) => {
        val vectorSum = DenseVector(left.factors) + DenseVector(right.factors)
        val uID = left.id

        new Factors(uID, vectorSum.data)
      }
    }

    val uA = uVA.map(_._2).groupBy(0).reduce {
      (left, right) => {
        val t = DenseMatrix.create(factors, factors, left.factors)
        val matrix = DenseMatrix.create(factors, factors, left.factors) +
          DenseMatrix.create(factors, factors, right.factors)

        new Factors(left.id, matrix.data)
      }
    }

    uA.join(uV).where(0).equalTo(0) {
      (aMatrix, vVector) =>
        val uID = aMatrix.id
        val matrix = DenseMatrix.create(factors, factors, aMatrix.factors)
        val vector = DenseVector(vVector.factors)

        new Factors(uID, (matrix \ vector).data)
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
