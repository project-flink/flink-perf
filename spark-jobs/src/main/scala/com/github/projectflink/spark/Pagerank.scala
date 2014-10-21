package com.github.projectflink.spark


import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.zookeeper.KeeperException.SystemErrorException

import _root_.scala.collection.mutable


object Pagerank {

  def main(args: Array[String]): Unit = {
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
    val dop = args(6).toInt

    val dampingFactor = 0.85

    val conf = new SparkConf().setAppName("Spark pagerank").setMaster(master)
    conf.set("spark.hadoop.skipOutputChecks", "false")
    implicit val sc = new SparkContext(conf)

    val inData : RDD[String] = sc.textFile(input.toString)

    val adjacencyMatrix = inData.repartition(dop).map{ line =>
      val sp = line.split(" ").map(_.toInt)
      (sp(0), sp.tail)
    }

    val adjacencyMatrixCached = adjacencyMatrix.cache();
    var pagerank = adjacencyMatrixCached.map { tup =>
      (tup._1, 1.0/numVertices)
    }

    for(i <- 1 to maxIterations) {
      System.out.println("++++ Starting next iteration");
      pagerank = pagerank.join(adjacencyMatrixCached, dop).flatMap {
        case (node, (rank, neighboursIt)) => {
          val neighbours = neighboursIt.toSeq
          neighbours.map {
            (_, dampingFactor * rank / neighbours.length)
          } :+ (node, (1 - dampingFactor) / numVertices)
        }
      }.reduceByKey(_ + _, dop)
      //pagerank.foreach( println(_))

    }

    if(output != null) {
      pagerank.saveAsTextFile(output+"_spark")
    }else{
      pagerank.foreach(println _)
    }
  }

}
