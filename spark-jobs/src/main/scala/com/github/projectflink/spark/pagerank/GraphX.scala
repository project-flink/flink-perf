package com.github.projectflink.spark.pagerank

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx.GraphLoader

object GraphX {
  def main(args: Array[String]) {

    val master = args(0)
    val maxIterations = args(1).toInt
    val input = args(2)
    val output = args(3)

    print(s"GraphX pageRank master=$master maxIter=$maxIterations input=$input output=$output")

    val conf = new SparkConf().setAppName("Spark GraphX PageRank").setMaster(master)
    conf.set("spark.hadoop.skipOutputChecks", "true")
    implicit val sc = new SparkContext(conf)

    // Load the edges as a graph
    val graph = GraphLoader.edgeListFile(sc, input)
    // Run PageRank
    val ranks = graph.staticPageRank(maxIterations).vertices

    ranks.saveAsTextFile(output)
  }
}
