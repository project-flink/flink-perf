package com.github.projectflink.spark

import org.apache.spark.rdd.RDD

import scala.collection.mutable

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import scala.util.Random

object GroupReduceBenchmarkSpark {

  def main(args: Array[String]) {
    if (args.length < 4) {
      println("Usage: [master] [dop] [k] [input] [output]")
      return
    }

    var master = args(0)
    var dop = args(1).toInt
    var k = args(2).toInt
    var inputPath = args(3)
    val outputPath = if (args.length > 4) {
      args(4)
    } else {
      null
    }

    // set up the execution environment
    val conf = new SparkConf()
      .setAppName("Group Reduce Benachmark")
      .setMaster(master)
      .set("spark.default.parallelism", dop.toString)
    val spark = new SparkContext(conf)

    val readsWithCountryAndBook = spark.textFile(inputPath, dop) map {
      in =>
        val Array(country, book) = in.split(',')
        (country, book)
    }

    val result = readsWithCountryAndBook
      .map { case (country, book) => ((country, book), 1L) }
      .reduceByKey { _ + _ }
      .map { case ((country, book), count) => (country, (book, count)) }
      .groupByKey
      .map { case (country, songs) =>
        (country, songs.toList.sortBy(- _._2).take(k).mkString(", "))
      }

    if (outputPath == null) {
      result foreach { println(_) }
    } else {
      result.saveAsTextFile(outputPath + "_spark")
    }

  }
}

