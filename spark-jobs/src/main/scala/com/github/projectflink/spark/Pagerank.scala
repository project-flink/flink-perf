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

    val threshold: Double = 0.005 / numVertices

    val conf = new SparkConf().setAppName("Spark pagerank").setMaster(master)
    conf.set("spark.hadoop.skipOutputChecks", "false")
    implicit val sc = new SparkContext(conf)

    val inData : RDD[String] = sc.textFile(input.toString)

    val adjacencyMatrix = inData.repartition(dop).map{ line =>
      val sp = line.split(" ").map(_.toInt)
      (sp(0), sp.tail)
    }

    val adjacencyMatrixCached = adjacencyMatrix.cache();
    var inPagerank = adjacencyMatrixCached.map { tup =>
      (tup._1, 1.0/numVertices)
    }

    var i = 0
    var terminated = false;
    while( i < maxIterations && !terminated) {
      i = i + 1
      System.out.println("++++ Starting next iteration");
      val outPagerank = inPagerank.join(adjacencyMatrixCached, dop).flatMap {
        case (node, (rank, neighboursIt)) => {
          val neighbours = neighboursIt.toSeq
          neighbours.map {
            (_, dampingFactor * rank / neighbours.length)
          } :+ (node, (1 - dampingFactor) / numVertices)
        }
      }.reduceByKey(_ + _, dop)

      //compute termination criterion
      val count = outPagerank.join(inPagerank).flatMap {
        case (node, (r1, r2)) => {
          val delta = Math.abs(r1 - r2);
          if(delta > threshold) {
            Some(1)
          }else{
            None
          }
        }
      }.count()

      print("count = "+count+" at iteration "+i)
      if(count == 0) {
        terminated = true;
      }
      // set new inPr
      inPagerank = outPagerank

    }

    // inPagerank is the outPageRank at this point.
    if(output != null) {
      inPagerank.saveAsTextFile(output+"_spark")
    }else{
      inPagerank.foreach(println _)
    }
  }

}
