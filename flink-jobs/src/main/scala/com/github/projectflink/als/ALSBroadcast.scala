package com.github.projectflink.als

import java.lang.Iterable

import breeze.linalg.{diag, DenseVector, DenseMatrix}
import breeze.stats.distributions.{Rand, ThreadLocalRandomGenerator}
import com.github.projectflink.als.ALSUtils.{BreezeVector, ALSConfig}
import org.apache.flink.api.common.functions.RichGroupReduceFunction
import org.apache.commons.math3.random.MersenneTwister
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.util.Collector
import scala.reflect.io.Path
import org.apache.flink.api.scala._

object ALSBroadcast extends ALSBase {

  val BROADCAST_M = "m"
  val BROADCAST_U = "u"

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[ALSConfig]("ALS"){
      head("ALS benchmark", "1.0")
      arg[Int]("numListeners") action {
        (v, c) => c.copy(numListeners = v)
      } text {
        "Number of listeners"
      }
      arg[Int]("numSongs") action {
        (v ,c) => c.copy(numSongs = v)
      } text {
        "Number of songs"
      }
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

        val updateU = new RichGroupReduceFunction[(Int, Int, Double), BreezeVector]() {
          import scala.collection.JavaConverters._
          var m: DenseMatrix[Double] = null

          override def open(config: Configuration): Unit ={

            m = DenseMatrix.zeros[Double](numLatentVariables, numSongs)

            for(vector <- getRuntimeContext.getBroadcastVariable[BreezeVector](BROADCAST_M).asScala){
              m(::, vector.idx-1) := vector.wrapper.vector
            }
          }

          override def reduce(rankings: Iterable[(Int, Int, Double)], out: Collector[BreezeVector]):
          Unit = {
            var n = 1

            val it = rankings.iterator()
            val(listener, firstSong, firstRanking) = it.next

            val firstVector:DenseVector[Double] = m(::, firstSong-1)
            val v:DenseVector[Double] = firstVector * firstRanking
            import outerProduct._
            val A = outerProduct(firstVector, firstVector)

            while(it.hasNext){
              val (_, song, ranking) = it.next()

              val mVector:DenseVector[Double] = m(::, song-1)
              v += mVector * ranking
              A += outerProduct(mVector, mVector)

              n += 1
            }

            diag(A) += lambda * n

            out.collect(BreezeVector(listener, A \ v))
          }
        }

        val updateM = new RichGroupReduceFunction[(Int, Int, Double), BreezeVector]() {
          import scala.collection.JavaConverters._
          var u: DenseMatrix[Double] = null

          override def open(config: Configuration): Unit = {
            u = DenseMatrix.zeros[Double](numLatentVariables, numListeners)

            for(vector <- getRuntimeContext.getBroadcastVariable[BreezeVector](BROADCAST_U).asScala){
              u(::, vector.idx-1) := vector.wrapper.vector
            }
          }

          override def reduce(rankings: Iterable[(Int, Int, Double)], out: Collector[BreezeVector]):
          Unit = {
            var n = 1

            val it = rankings.iterator()
            val(firstListener, song, firstRanking) = it.next

            val firstVector = u(::, firstListener-1)

            val v: DenseVector[Double] = firstVector*firstRanking

            import outerProduct._
            val A = outerProduct(firstVector, firstVector)

            while(it.hasNext){
              val(listener, _, ranking) = it.next()

              val vector = u(::, listener-1)

              v += vector * ranking
              A += outerProduct(vector, vector)

              n += 1
            }

            diag(A) += lambda*n

            out.collect(BreezeVector(song, A \ v))
          }
        }

        val m = initialM.iterate(maxIterations){
          m => {
            val u = rankings.groupBy(0).reduceGroup(updateU).withBroadcastSet(m, BROADCAST_M)
            rankings.groupBy(1).reduceGroup(updateM).withBroadcastSet(u, BROADCAST_U)
          }
        }

        val u = rankings.groupBy(0).reduceGroup(updateU).withBroadcastSet(m, BROADCAST_M)


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

}
