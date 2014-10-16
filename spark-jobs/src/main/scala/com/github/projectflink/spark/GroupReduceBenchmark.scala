package com.github.projectflink.spark

import org.apache.spark.rdd.RDD

import scala.collection.mutable

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import scala.util.Random

object GroupReduceBenchmarkSpark {
  val rnd = new Random(System.currentTimeMillis())

  private final def skewedSample(skew: Double, max: Int): Int = {
    val uniform = rnd.nextDouble()
    val `var` = Math.pow(uniform, skew)
    val pareto = 0.2 / `var`
    val `val` = pareto.toInt
    if (`val` > max) `val` % max else `val`
  }

  def main(args: Array[String]) {
    if (args.length < 7) {
      println("Usage: [master] [dop] [numCountries] [numBooks] [numReads] [skew] [k] [output]")
      return
    }

    var master = args(0)
    var dop = args(1).toInt
    var numCountries = args(2).toInt
    var numBooks = args(3).toInt
    var numReads = args(4).toInt
    var skew = args(5).toFloat
    var k = args(6).toInt
    val outputPath = if (args.length > 7) {
      args(7)
    } else {
      null
    }

    val hash = mutable.HashMap[Int, Int]()

    for (i <- 0 to numReads) {
      val key = skewedSample(skew, numCountries - 1)
      val prev: Int = hash.getOrElse(key, 0)
      hash.put(key, prev + 1)
    }

    val readsInCountry = hash.values.toArray


    // set up the execution environment
    val conf = new SparkConf()
      .setAppName("Group Reduce Benachmark")
      .setMaster("local")
      .set("spark.default.parallelism", dop.toString)
    val spark = new SparkContext(conf)

    val countryIds: RDD[Int] = spark.parallelize(0 to (numCountries - 1))
    val bookIds: RDD[Int] = spark.parallelize(0 to (numBooks - 1))

    val countryNames = countryIds map { id => (id, rnd.nextString(20)) }
    val bookNames = bookIds map { id => (id, rnd.nextString(30)) }

    val reads = countryIds flatMap {
      id =>
        val numReads = readsInCountry(id)
        (0 to numReads) map { i => (id, rnd.nextInt(numBooks)) }
    }


    val readsWithCountry = reads.join(countryNames) map {
      case (key, bookIdCountry) => bookIdCountry
    }

    val readsWithCountryAndBook = readsWithCountry.join(bookNames) map {
      case (key, countryWithBook) => countryWithBook
    }

    //    println(readsWithCountryAndBook.collect().mkString(", "))

    val result = readsWithCountryAndBook
      .map { case (country, book) => ((country, book), 1) }
      .reduceByKey { _ + _ }
      .map { case ((country, book), count) => (country, (book, count)) }
      .groupByKey
      .map { case (country, songs) =>
        (country, songs.toList.sortBy(- _._2).take(5).mkString(", "))
      }

    if (outputPath == null) {
      result foreach { println(_) }
    } else {
      result.saveAsTextFile(outputPath + "_spark")
    }

  }
}

