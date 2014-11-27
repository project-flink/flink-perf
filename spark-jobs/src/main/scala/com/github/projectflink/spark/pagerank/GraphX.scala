package com.github.projectflink.spark.pagerank

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx.GraphLoader

object GraphX {
  def main(args: Array[String]) {

    val master = args(0)
    val numVertices = args(1).toInt
    val sparsity = args(2).toDouble
    val maxIterations = args(3).toInt
    val output = if(args.length > 4){
      args(4)
    }else{
      null
    }
    val input = args(5)

    val threshold: Double = 0.005 / numVertices

    val conf = new SparkConf().setAppName("Spark GraphX PageRank").setMaster(master)
    conf.set("spark.hadoop.skipOutputChecks", "false")
    implicit val sc = new SparkContext(conf)

    // Load the edges as a graph
    val graph = GraphLoader.edgeListFile(sc, input)
    // Run PageRank
    val ranks = graph.pageRank(maxIterations).vertices

    ranks.saveAsTextFile(output)
  }
}
