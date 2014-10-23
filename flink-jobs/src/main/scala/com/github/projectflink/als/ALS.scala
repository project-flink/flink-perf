package com.github.projectflink.als

import breeze.linalg._
import breeze.stats.distributions.{ThreadLocalRandomGenerator, RandBasis, Rand}
import com.github.projectflink.als.ALSUtils._
import org.apache.commons.math3.random.MersenneTwister
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

import scala.reflect.io.Path

object ALS extends ALSBase {

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[ALSConfig]("ALS"){
      head("ALS benchmark", "1.0")
      arg[Int]("numLatentVariables") action {
        (v, c) => c.copy(numLatentVariables = v)
      } text {
        "Number of latent variables of the approximation"
      }
      arg[Double]("lambda") action {
        (v, c) => c.copy(lambda = v)
      } text {
        "Regularization constant"
      }
      arg[Int]("maxIterations") action {
        (v, c) => c.copy(maxIterations = v)
      } text{
        "Maximal number of iterations"
      }
      arg[String]("inputRankings") optional() action {
        (v, c) => c.copy(inputRankings = v)
      } text{
        "Path to file containing input ranking matrix."
      }
      arg[String]("ouputPath") optional() action{
        (v, c) => c.copy(outputPath = v)
      } text{
        "Path to output file"
      }
    }

    parser.parse(args, ALSConfig()) map {
      config =>
        import config._
        val env = ExecutionEnvironment.getExecutionEnvironment

        val rankings = if(inputRankings == null){
          val (i, _, _) = ALSDataGeneration.generateData(defaultDataGenerationConfig, env)
          i
        }else{
          val path = Path(inputRankings) / ALSUtils.RANKING_MATRIX
          env.readCsvFile[(Int, Int, Double)](
          filePath = path.toString,
          lineDelimiter =  "\n",
          fieldDelimiter = ','
          )
        }

        val adjacencies = rankings map { x => (x._1, x._2) }

        val initialM = rankings map {
          x => (x._2, x._3, 1)
        } groupBy(0) reduce{
          (left, right) => (left._1, left._2 + right._2, left._3 + right._3)
        } map {
          x =>
            val (mID, rankSum, numberNonZeroRanks) = x
            val average = rankSum/numberNonZeroRanks
//            val random = Rand.uniform
            val generator = new ThreadLocalRandomGenerator(new MersenneTwister(mID))
            val random = new Rand[Double]{
              override def draw(): Double = generator.nextDouble()
            }

            val t = random.sample(numLatentVariables-1).map{ _ * average/SCALING_FACTOR}
            BreezeVector(mID, average +: t)
        }

        val m = initialM.iterate(maxIterations){
          m => {
            val u = updateU(rankings, m, adjacencies, lambda)
            updateM(rankings, u, adjacencies, lambda)
          }
        }

        val u = updateU(rankings, m, adjacencies, lambda)

        if(outputPath != null){
          val ufile = Path(outputPath) / U_MATRIX_FILE
          u.writeAsCsv(
            ufile.toString(),
            "\n",
            ",",
            WriteMode.OVERWRITE
          )

          val mfile = Path(outputPath) / M_MATRIX_FILE

          m.writeAsCsv(
            mfile.toString,
            "\n",
            ",",
            WriteMode.OVERWRITE
          )
        }else{
          u.print()
          m.print()
        }

        env.execute("ALS benchmark")
    } getOrElse{
      println("Error parsing the command line options.")
    }
  }

  def updateU(rankings: CellMatrix, m: ColMatrix, adjacencies: AdjacencyMatrix,
              lambda: Double): DataSet[BreezeVector] = {
    val uV = m.join(rankings).where(0).equalTo(1) {
      (mVector, rankingEntry) => {
        val (uID, _, ranking) = rankingEntry
        val vector = mVector.wrapper.vector

        BreezeVector(uID, vector * ranking)
      }
    }.groupBy(0).reduce{
      (left, right) => {
        val (BreezeVector(uID, leftWrapper), BreezeVector(_, rightWrapper)) = (left, right)

        BreezeVector(uID, leftWrapper.vector + rightWrapper.vector)
      }
    }

    val uA = m.join(adjacencies).where(0).equalTo(1){
      (mVector, adjacencyRelation) => {
        val uID = adjacencyRelation._1
        val vector = mVector.wrapper.vector

        import outerProduct._
        val partialMatrix = outerProduct(vector, vector)

        diag(partialMatrix) += lambda

        BreezeMatrix(uID, partialMatrix)
      }
    }.groupBy(0).reduce{
      (left, right) => BreezeMatrix(left.idx, left.wrapper.matrix + right.wrapper.matrix)
    }

    uA.join(uV).where(0).equalTo(0){
      (aMatrix, vVector) =>
        val (BreezeMatrix(uID, BreezeDenseMatrixWrapper(matrix))) = aMatrix
        val (BreezeVector(_, BreezeDenseVectorWrapper(vector))) = vVector

        BreezeVector(uID, matrix \ vector)
    }
  }

  def updateM(rankings: CellMatrix, u: ColMatrix, adjacencies: AdjacencyMatrix, lambda: Double
               ): DataSet[BreezeVector] = {
    val mV = u.join(rankings).where(0).equalTo(0){
      (uVector, rankingEntry) => {
        val (_, mID, ranking) = rankingEntry
        val (BreezeVector(_, BreezeDenseVectorWrapper(vector))) = uVector

        BreezeVector(mID, vector * ranking)
      }
    }.groupBy(0).reduce{
      (left, right) => {
        val (BreezeVector(mID, BreezeDenseVectorWrapper(leftVector)), BreezeVector(_,
        BreezeDenseVectorWrapper(rightVector))) = (left,right)

        BreezeVector(mID, leftVector + rightVector)
      }
    }

    val mA = u.join(adjacencies).where(0).equalTo(0){
      (uVector, adjacencyRelation) => {
        val vector = uVector.wrapper.vector
        val mID = adjacencyRelation._2

        import outerProduct._
        val partialMatrix = outerProduct(vector, vector)

        diag(partialMatrix) += lambda

        BreezeMatrix(mID, partialMatrix)
      }
    }.groupBy(0).reduce{
      (left, right) => {
        val (BreezeMatrix(mID, BreezeDenseMatrixWrapper(leftMatrix)), BreezeMatrix(_,
        BreezeDenseMatrixWrapper(rightMatrix))) = (left, right)

        BreezeMatrix(mID, leftMatrix + rightMatrix)
      }
    }

    mA.join(mV).where(0).equalTo(0){
      (aMatrix, vVector) => {
        val BreezeMatrix(mID, BreezeDenseMatrixWrapper(matrix)) = aMatrix
        val vector = vVector.wrapper.vector

        BreezeVector(mID, matrix \ vector)
      }
    }
  }

}
