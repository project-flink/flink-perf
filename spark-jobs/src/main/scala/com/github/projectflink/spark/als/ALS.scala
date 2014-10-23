package com.github.projectflink.spark.als

import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser

object ALS {
  case class ALSConfig(master: String = "local[4]", numLatentVariables: Int = 0,
                       lambda: Double = 0.0, maxIterations: Int = 0,
                       inputRankings: String = null, outputPath: String = null)

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[ALSConfig]("ALS"){
      head("ALS benchmark", "1.0")
      arg[String]("master") action {
        (v, c) => c.copy(master = v)
      } text {
        "Spark master URL"
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
      config => {
        import config._

        val conf = new SparkConf().setMaster(master).setAppName("ALS")
        conf.set("spark.hadoop.skipOutputChecks", "false")

        val sc = new SparkContext(conf)


      }
    } getOrElse {
      println("Error while parsing the command line options.")
    }
  }
}
