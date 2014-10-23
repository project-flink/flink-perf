package com.github.projectflink.als

import breeze.linalg.{DenseMatrix, diag, DenseVector}
import com.github.projectflink.common.als.{outerProduct, Factors, Rating}
import org.apache.flink.api.scala._


class ALSJoin(users: Int, items: Int, factors: Int, lambda: Double,
              iterations: Int, seed: Long) extends ALSFlinkAlgorithm with
Serializable {

  def factorize(ratings: DS[RatingType]): Factorization = {
    val randomSeed = if(seed == -1) System.currentTimeMillis() else seed

    val itemIDs = ratings map { x => Tuple1(x.item) } distinct

    val initialItemMatrix = generateRandomMatrix(itemIDs map { _._1 }, factors, randomSeed)

    val itemMatrix = initialItemMatrix.iterate(iterations){
      itemMatrix => {
        val userMatrix = updateU(ratings, itemMatrix, lambda)
        updateI(ratings, userMatrix, lambda)
      }
    }

    val userMatrix = updateU(ratings, itemMatrix, lambda)

    Factorization(userMatrix, itemMatrix)
  }

  def updateU(ratings: DataSet[RatingType], items: DataSet[FactorType],
              lambda: Double): DataSet[FactorType] = {
    val uV = items.join(ratings).where(0).equalTo(1) {
      (item, ratingEntry) => {
        val Rating(uID, _, rating) = ratingEntry
        val vector:DenseVector[ElementType] = DenseVector(item.factors) * rating

        new Factors(uID, vector.data)
      }
    }.groupBy(0).reduce{
      (left, right) => {
        val vectorSum = DenseVector(left.factors) + DenseVector(right.factors)
        val uID = left.id

        new Factors(uID, vectorSum.data)
      }
    }

    val uA = items.join(ratings).where(0).equalTo(1){
      (item, ratingEntry) => {
        val uID = ratingEntry.user
        val vector = DenseVector(item.factors)
        import outerProduct._
        val partialMatrix = outerProduct(vector, vector)

        diag(partialMatrix) += lambda

        new Factors(uID, partialMatrix.data)
      }
    }.groupBy(0).reduce{
      (left, right) => {
        val t = DenseMatrix.create(factors, factors, left.factors)
        val matrix = DenseMatrix.create(factors, factors, left.factors) +
          DenseMatrix.create(factors,factors, right.factors)

        new Factors(left.id, matrix.data)
      }
    }

    uA.join(uV).where(0).equalTo(0){
      (aMatrix, vVector) =>
        val uID = aMatrix.id
        val matrix = DenseMatrix.create(factors, factors, aMatrix.factors)
        val vector = DenseVector(vVector.factors)

        new Factors(uID, (matrix \ vector).data)
    }
  }

  def updateI(ratings: DS[RatingType], u: DS[FactorType], lambda: Double): DS[FactorType] = {
    val itemV = u.join(ratings).where(0).equalTo(0){
      (user, ratingEntry) => {
        val Rating(_, itemID, ranking) = ratingEntry
        val vector:DenseVector[ElementType] = DenseVector(user.factors) * ranking

        new Factors(itemID, vector.data)
      }
    }.groupBy(0).reduce{
      (left, right) => {
        val itemID = left.id
        val vectorSum = DenseVector(left.factors) + DenseVector(right.factors)

        new Factors(itemID, vectorSum.data)
      }
    }

    val itemA = u.join(ratings).where(0).equalTo(0){
      (user, ratingEntry) => {
        val vector = DenseVector(user.factors)
        val itemID = ratingEntry.item

        import outerProduct._
        val partialMatrix = outerProduct(vector, vector)

        diag(partialMatrix) += lambda

        new Factors(itemID, partialMatrix.data)
      }
    }.groupBy(0).reduce{
      (left, right) => {
        val matrixSum = DenseMatrix.create(factors, factors, left.factors) + DenseMatrix.create(
          factors, factors, right.factors)

        new Factors(left.id, matrixSum.data)
      }
    }

    itemA.join(itemV).where(0).equalTo(0){
      (aMatrix, vVector) => {
        val itemID = aMatrix.id
        val matrix = DenseMatrix.create(factors, factors, aMatrix.factors)
        val vector = DenseVector(vVector.factors)

        new Factors(itemID, (matrix \ vector).data)
      }
    }
  }
}

object ALSJoin extends ALSFlinkRunner with ALSFlinkToyRatings{
  def main(args: Array[String]): Unit = {
    parseCL(args) map {
      config => {
        import config._

        val env = ExecutionEnvironment.getExecutionEnvironment
        val ratings = readRatings(inputRatings, env)

        val als = new ALSJoin(users, items, factors, lambda, iterations, seed)
        val factorization = als.factorize(ratings)

        outputFactorization(factorization, outputPath)

        env.setDegreeOfParallelism(2)
        env.execute("ALS benchmark")
      }
    } getOrElse{
      println("Could not parse command line parameters.")
    }
  }
}
