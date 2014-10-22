package com.github.projectflink.als

import breeze.linalg._
import breeze.stats.distributions.Rand
import com.github.projectflink.als.ALSUtils._
import org.apache.flink.api.scala._

import scala.reflect.io.Path

object ALS {
  val defaultDataGenerationConfig = ALSDGConfig(10, 15, 5, 10, 1, 4, 1)
  val SCALING_FACTOR = 100

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

        rankings.print()

        val strippedRankings = rankings map { x => (x._1, x._2) }

        val initialM = rankings map {
          x => (x._2, x._3, 1)
        } groupBy(0) reduce{
          (left, right) => (left._1, left._2 + right._2, left._3 + right._3)
        } map {
          x =>
            val (mID, rankSum, numberNonZeroRanks) = x
            val average = rankSum/numberNonZeroRanks
            val random = Rand.uniform

            val t = random.sample(numLatentVariables-1).map{ _ * average/SCALING_FACTOR}
            BreezeVector(mID, average +: t)
        }

        initialM.print()

        val uV = initialM.join(rankings).where(0).equalTo(1).map{
          x => {
            val (BreezeVector(_, vWrapper), (uID, _, ranking)) = x

            BreezeVector(uID, vWrapper.vector * ranking)
          }
        }.groupBy(0).reduce{
          (left, right) => {
            val (BreezeVector(uID, leftWrapper), BreezeVector(_, rightWrapper)) = (left, right)

            BreezeVector(uID, leftWrapper.vector + rightWrapper.vector)
          }
        }

        val uA = initialM.join(strippedRankings).where(0).equalTo(1).map {
          x => {
            val (BreezeVector(_, BreezeDenseVectorWrapper(vector)), (uID, _)) = x
            import outerProduct._
            val partialMatrix = outerProduct(vector, vector)

            diag(partialMatrix) += lambda

            BreezeMatrix(uID, partialMatrix)
          }
        }.groupBy(0).reduce{
          (left, right) => BreezeMatrix(left.idx, left.wrapper.matrix + right.wrapper.matrix)
        }

        val u = uA.join(uV).where(0).equalTo(0).map{
          x =>
            val (BreezeMatrix(uID, matrix), BreezeVector(_, wrapper)) = x

            BreezeVector(uID, matrix \ wrapper.vector)
        }

        val mV = u.join(rankings).where(0).equalTo(0).map{
          x => {
            val (BreezeVector(_, wrapper), (_, mID, ranking)) = x

            BreezeVector(mID, wrapper * ranking)
          }
        }.groupBy(0).reduce{
          (left, right) => {
            val (BreezeVector(mID, BreezeDenseVectorWrapper(leftVector)), BreezeVector(_,
            BreezeDenseVectorWrapper(rightVector))) = (left,right)

            BreezeVector(mID, leftVector + rightVector)
          }
        }

        val mA = u.join(strippedRankings).where(0).equalTo(0).map{
          x => {
            val (BreezeVector(_, BreezeDenseVectorWrapper(vector)), (_, mID)) = x

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

        val m = mA.join(mV).where(0).equalTo(0).map{
          x => {
            val (BreezeMatrix(mID, BreezeDenseMatrixWrapper(matrix)), BreezeVector(_,
              BreezeDenseVectorWrapper(vector))) = x

            BreezeVector(mID, matrix \ vector)
          }
        }

        u.print()
        m.print()

        env.execute("ALS benchmark")
    } getOrElse{
      println("Error parsing the command line options.")
    }
  }

}
