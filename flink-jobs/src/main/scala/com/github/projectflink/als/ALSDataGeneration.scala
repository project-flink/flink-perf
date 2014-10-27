package com.github.projectflink.als

import java.net.URL

import breeze.stats.distributions.{RandBasis, ThreadLocalRandomGenerator, Gaussian, Rand}
import org.apache.commons.math3.random.MersenneTwister
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

import scala.reflect.io.Path

import breeze.linalg._

case class ALSDGConfig(numListeners: Int = 0, numSongs: Int = 0, numLatentVariables: Int = 0,
                       meanEntries: Double = 0,
                       varEntries: Double = 0,
                       meanNumRankingEntries: Double = 0,
                       varNumRankingEntries: Double = 0,
                       outputPath: String = null)

object ALSDataGeneration{
  val RANKING_MATRIX = "rankingMatrix"
  val SONGS_MATRIX = "songsMatrix"
  val LISTENERS_MATRIX = "listenersMatrix"

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[ALSDGConfig]("ALSDataGeneration"){
      head("ALS data generation", "1.0")
      arg[Int]("numListeners") action { (value, conf) => conf.copy(numListeners = value) } text {
        "Number of listeners."}
      arg[Int]("numSongs") action { (value, conf) => conf.copy(numSongs = value) } text {
        "Number of songs." }
      arg[Int]("numLatentVariables") action { (value, conf) => conf.copy(numLatentVariables =
        value)} text { "Number of latent variables." }
      arg[Double]("meanEntries") action { (value, conf) => conf.copy(meanEntries = value) } text
        { "Mean of normal distribution of generated entries."}
      arg[Double]("varEntries") action { (value, conf) => conf.copy(varEntries = value) } text {
        "Variance of normal distribution of generated entries."}
      arg[Double]("meanNumListenEntries") action {
        (value, conf) => conf.copy(meanNumRankingEntries = value)
      } text {
        "Normal distribution mean of number of non zero entries of ranking matrix."
      }
      arg[Double]("varNumListenEntries") action {
        (value, conf) => conf.copy(varNumRankingEntries = value)
      } text {
        "Normal distribution variance of number of non zero entries of ranking matrix."
      }
      arg[String]("outputPath") optional() action {
        (value, conf) => conf.copy(outputPath = value)
      } text {
        "Output path for generated data files."
      }
    }

    parser.parse(args, ALSDGConfig()) map { config =>

      val env = ExecutionEnvironment.getExecutionEnvironment

      val (ratingMatrix, listeners, songs) = generateData(config, env)

      import config._

      if(outputPath == null) {
        // print data
        ratingMatrix.print()
      }else{
        // write to disk
        ratingMatrix.writeAsCsv(
          filePath = outputPath + s"$numListeners-$numSongs-$numLatentVariables-$meanNumRankingEntries",
          rowDelimiter = "\n",
          fieldDelimiter = ",",
          writeMode = WriteMode.OVERWRITE
        )
      }

      env.execute("Generate data")
    } getOrElse{
      println("Error parsing the command line options.")
    }
  }

  def generateData(config: ALSDGConfig, env: ExecutionEnvironment) = {
    import config._

    val intermediateListeners = env.generateSequence(1, numListeners) map {
      x => {
        val rand = Rand.gaussian(meanEntries, varEntries)
        val entries = rand.sample(numLatentVariables)

        val threshold = Rand.gaussian(meanNumRankingEntries, varNumRankingEntries).draw/numSongs

        (x.toInt, entries.toArray, threshold)
      }
    }

    val listeners = intermediateListeners map { x => (x._1, x._2) }

    val songs = env.generateSequence(1, numSongs) map {
      x => {
        val rand = Rand.gaussian(meanEntries, varEntries)
        val entries = rand.sample(numLatentVariables)
        (x.toInt, entries.toArray)
      }
    }

    val rankingMatrix = intermediateListeners.cross(songs).flatMap {
      x =>
        val ((row, left, threshold), (col, right)) = x
        val rnd = Rand.uniform
        val prob = rnd.sample()

        if (prob <= threshold) {
          val leftVector = DenseVector(left)
          val rightVector = DenseVector(right)
          val result: Double = leftVector dot rightVector

          Some(row, col, result)
        } else {
          None
        }
    }

    (rankingMatrix, listeners, songs)
  }
}
