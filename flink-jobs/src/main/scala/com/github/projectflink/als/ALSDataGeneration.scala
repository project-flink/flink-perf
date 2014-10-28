package com.github.projectflink.als

import breeze.stats.distributions.Rand
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

import scala.util.Random

case class ALSDGConfig(numListeners: Int = 0, numSongs: Int = 0,
                       meanEntries: Double = 0,
                       stdEntries: Double = 0,
                       meanNumRankingEntries: Double = 0,
                       stdNumRankingEntries: Double = 0,
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
      arg[Double]("meanEntries") action { (value, conf) => conf.copy(meanEntries = value) } text
        { "Mean of normal distribution of generated entries."}
      arg[Double]("stdEntries") action { (value, conf) => conf.copy(stdEntries = value) } text {
        "Variance of normal distribution of generated entries."}
      arg[Double]("meanNumListenEntries") action {
        (value, conf) => conf.copy(meanNumRankingEntries = value)
      } text {
        "Normal distribution mean of number of non zero entries of ranking matrix."
      }
      arg[Double]("stdNumListenEntries") action {
        (value, conf) => conf.copy(stdNumRankingEntries = value)
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

      val ratingMatrix = generateData(config, env)

      import config._

      if(outputPath == null) {
        // print data
        ratingMatrix.print()
      }else{
        // write to disk
        ratingMatrix.writeAsCsv(
          filePath = outputPath + s"$numListeners-$numSongs-${meanNumRankingEntries.toInt}",
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

    val ratingMatrix = env.generateSequence(1, numListeners).flatMap {
      listenerID =>
        val random = new Random(System.currentTimeMillis() ^ (listenerID << 32))
//        val numEntries = random.nextGaussian()*stdNumRankingEntries + meanNumRankingEntries
//        val bernoulliProb = numEntries/numSongs
//
//        val songs = (1 to numSongs).filter( _ => random.nextDouble() <= bernoulliProb ).map {
//          songID => {
//            (listenerID.toInt, songID, random.nextGaussian()*stdEntries + meanEntries)
//          }
//        }
//        songs
        (1 to numSongs) map {
          songID => {
            (listenerID.toInt, songID, 1)
          }
        }
    }
    ratingMatrix
  }
}
