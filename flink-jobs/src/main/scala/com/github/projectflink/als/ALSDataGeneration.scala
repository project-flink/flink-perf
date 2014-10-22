package com.github.projectflink.als

import breeze.stats.distributions.{RandBasis, ThreadLocalRandomGenerator, Gaussian, Rand}
import org.apache.commons.math3.random.MersenneTwister
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

import scala.reflect.io.Path

import breeze.linalg._

object ALSDataGeneration{
  import ALSUtils._

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
      arg[Double]("stdEntries") action { (value, conf) => conf.copy(varEntries = value) } text {
        "Variance of normal distribution of generated entries."}
      arg[Double]("meanNumListenEntries") action {
        (value, conf) => conf.copy(meanNumRankingEntries = value)
      } text {
        "Normal distribution mean of number of non zero entries of ranking matrix."
      }
      arg[Double]("stdNumListenEntries") action {
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

      val (rankingMatrix, listeners, songs) = generateData(config, env)

      if(config.outputPath == null) {
        // print data
        songs.print()
        listeners.print()
        rankingMatrix.print()
      }else{
        // write to disk
        val path = Path(config.outputPath)
        val rankingPath = path / ALSUtils.RANKING_MATRIX
        val songsPath = path / ALSUtils.SONGS_MATRIX
        val listenersPath = path / ALSUtils.LISTENERS_MATRIX

        rankingMatrix.writeAsCsv(
          filePath = rankingPath.toString,
          rowDelimiter = "\n",
          fieldDelimiter = ",",
          writeMode = WriteMode.OVERWRITE
        )

        songs.writeAsCsv(
          filePath = songsPath.toString,
          rowDelimiter = "\n",
          fieldDelimiter = ",",
          writeMode = WriteMode.OVERWRITE
        )

        listeners.writeAsCsv(
          filePath = listeners.toString,
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
//        val rand = Rand.gaussian(meanEntries, varEntries)
        val rand = Gaussian(meanEntries, varEntries)(new RandBasis(new ThreadLocalRandomGenerator
(new MersenneTwister(x))))
        val entries = rand.sample(numLatentVariables)

//        val threshold = Rand.gaussian(meanNumRankingEntries, varNumRankingEntries).draw/numSongs

        val threshold = Gaussian(meanNumRankingEntries, varNumRankingEntries)(new RandBasis(new
            ThreadLocalRandomGenerator(new MersenneTwister(x)))).draw/numSongs

        (BreezeVector(x.toInt, entries.toArray), threshold)
      }
    }

    val listeners = intermediateListeners map { x => x._1 }

    val songs = env.generateSequence(1, numSongs) map {
      x => {
//        val rand = Rand.gaussian(meanEntries, varEntries)
val rand = Gaussian(meanEntries, varEntries)(new RandBasis(new ThreadLocalRandomGenerator
(new MersenneTwister(x))))
        val entries = rand.sample(numLatentVariables)
        BreezeVector(x.toInt, entries.toArray)
      }
    }

    val rankingMatrix = intermediateListeners.cross(songs).flatMap {
      x =>
        val ((BreezeVector(row, left), threshold), BreezeVector(col, right)) = x
//        val rnd = Rand.uniform
        val rnd =  new ThreadLocalRandomGenerator(new MersenneTwister(row*numSongs + col))
        val prob = rnd.nextDouble()

        if (prob <= threshold) {
          val result: Double = left.vector dot right.vector

          Some(row, col, result)
        } else {
          None
        }
    }

    (rankingMatrix, listeners, songs)
  }
}
