package com.github.projectflink.common.als

import scopt.OptionParser

trait ALSRunner extends ALS {
  type Context

  val USER_FACTORS_FILE = "userFactorsFile"
  val ITEM_FACTORS_FILE = "itemFactorsFile"


  case class ALSConfig(master: String = "local[4]", users: Int = -1, items: Int = -1,
                       factors: Int = -1, lambda: Double = 0.0,
                       iterations: Int = 0, inputRatings: String = null, outputPath: String = null,
                       seed: Long = -1L, blocks: Int = -1)

  def readRatings(input: String, ctx: Context): DS[RatingType]

  def parseCL(args: Array[String]): Option[ALSConfig] = {
    val parser = new OptionParser[ALSConfig]("ALS"){
      head("ALS", "1.0")
      opt[String]('m', "master") optional() action {
        (v, c) => c.copy(master = v)
      } text {
        "Master URL"
      }
      opt[Int]('u', "users") optional() action {
        (v, c) => c.copy(users = v)
      } text {
        "Number of users"
      }
      opt[Int]("items") optional() action {
        (v, c) => c.copy(items = v)
      } text {
        "Number of items"
      }
      opt[Int]('f', "factors") action {
        (v, c) => c.copy(factors = v)
      } text {
        "Number of factors"
      }
      opt[Double]('r', "regularization") action {
        (v, c) => c.copy(lambda = v)
      } text {
        "Regularization constant"
      }
      opt[Int]('i', "iterations") action {
        (v, c) => c.copy(iterations = v)
      }
      opt[String]("input") action {
        (v, c) => c.copy(inputRatings = v)
      } text {
        "Path to input ratings"
      }
      opt[String]("output") action {
        (v, c) => c.copy(outputPath = v)
      } text {
        "Output path for the results"
      }
      opt[Long]('s', "seed") action {
        (v, c) => c.copy(seed = v)
      } text {
        "Seed for random initialization"
      }
      opt[Int]('b', "blocks") action {
        (v, c) => c.copy(blocks = v )
      } text {
        "Number of blocks"
      }
    }

    parser.parse(args, ALSConfig())
  }
}
