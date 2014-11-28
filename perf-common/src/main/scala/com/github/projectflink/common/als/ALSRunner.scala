package com.github.projectflink.common.als

import scopt.OptionParser

trait ALSRunner extends ALS {
  type Context

  val USER_FACTORS_FILE = "userFactorsFile"
  val ITEM_FACTORS_FILE = "itemFactorsFile"


  case class ALSConfig(master: String = "local[4]",
                       factors: Int = -1, lambda: Double = 0.0,
                       iterations: Int = 0, inputRatings: String = null, outputPath: String = null,
                       blocks: Int = -1, seed: Long = -1, persistencePath: Option[String] = None)

  def readRatings(input: String, ctx: Context): DS[RatingType]

  def parseCL(args: Array[String]): Option[ALSConfig] = {
    val parser = new OptionParser[ALSConfig]("ALS"){
      head("ALS", "1.0")
      arg[String]("master") action {
        (v, c) => c.copy(master = v)
      } text {
        "Master URL"
      }
      arg[Int]("factors") action {
        (v, c) => c.copy(factors = v)
      } text {
        "Number of factors"
      }
      arg[Double]("regularization") action {
        (v, c) => c.copy(lambda = v)
      } text {
        "Regularization constant"
      }
      arg[Int]("iterations") action {
        (v, c) => c.copy(iterations = v)
      }
      arg[Int]("blocks") action {
        (v, c) => c.copy(blocks = v )
      } text {
        "Number of blocks"
      }
      arg[String]("seed") action {
        (v, c) => {
          if(v.startsWith("rand")){
            c.copy(seed = System.currentTimeMillis())
          }else{
            c.copy(seed = v.toLong)
          }

        }
      } text {
        "Seed for random initialization"
      }
      arg[String]("persistencePath") optional() action {
        (v, c) => {
          if(!v.toLowerCase.equals("none")){
            c.copy(persistencePath = Some(if(v.endsWith("/")) v else v+"/"))
          }else{
            c
          }
        }
      } text {
        "Persistence path for the preprocessing data"
      }
      arg[String]("input") optional() action {
        (v, c) => c.copy(inputRatings = v)
      } text {
        "Path to input ratings"
      }
      arg[String]("output") optional() action {
        (v, c) => c.copy(outputPath = v)
      } text {
        "Output path for the results"
      }
    }

    parser.parse(args, ALSConfig())
  }
}
