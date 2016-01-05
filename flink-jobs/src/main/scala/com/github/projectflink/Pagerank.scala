package com.github.projectflink

import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.java.io.CsvInputFormat
import org.apache.flink.api.java.typeutils.TypeInfoParser
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.core.fs.Path
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.util.Random

/**
 * Dataset source : http://konect.uni-koblenz.de/networks/twitter
 *
 * File format
 * [from] [to]
 *
 * Example: (first  lines)
% asym unweighted
1 2
1 3
1 4
1 5
1 6
1 7

   41,652,230 vertices (users)
1,468,365,182 edges (followings)

 *
 *
 */
object Pagerank {
  case class Pagerank(node: Int, rank: Double){
    def +(pagerank: Pagerank) = {
      Pagerank(node, rank + pagerank.rank)
    }
  }
  case class AdjacencyRow(node: Int, neighbours: Array[Int]){
    override def toString: String = {
      s"$node: ${neighbours.mkString(",")}"
    }
  }

  var numVertices = 10
  val dampingFactor = 0.85
  var maxIterations = 10

  def main(args: Array[String]) {
    val inPath = args(0)
    val outPath = args(1)
    numVertices = args(2).toInt
    maxIterations = args(3).toInt
    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    val inData = env.readTextFile(inPath)
    val adjacencyMatrix = inData.map { line =>
      val sp = line.split(" ").map(_.toInt)
      AdjacencyRow(sp(0), sp.tail)
    }
    val initialPagerank = adjacencyMatrix.map { (tup: AdjacencyRow) =>
      Pagerank(tup.node, 1.0d/numVertices)
    }

    val solution = initialPagerank.iterate(maxIterations) {
        _.join(adjacencyMatrix, JoinHint.REPARTITION_HASH_FIRST).where("node").equalTo("node")
          .flatMap {
          _ match{
            case (Pagerank(node, rank), AdjacencyRow(_, neighbours)) =>{
              val length = neighbours.length
              (neighbours map {
                Pagerank(_, dampingFactor*rank/length)
              }) :+ Pagerank(node, (1-dampingFactor)/numVertices)
            }
          }
        }.groupBy("node").reduce(_ + _).withForwardedFields("0->0")
    }

    solution.writeAsText(outPath+"_flink", WriteMode.OVERWRITE)

    env.execute("Page Rank Flink Scala")
  }
}