package com.github.projectflink

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

import scala.util.Random

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
  var sparsity = 0.5

  def main(args: Array[String]) {
    // set up the execution environment
    numVertices = args(0).toInt
    sparsity = args(1).toDouble

    val env = ExecutionEnvironment.getExecutionEnvironment

    val adjacencyMatrix = getInitialAdjacencyMatrix(numVertices, env)
    val initialPagerank = getInitialPagerank(numVertices, env)

    val solution = initialPagerank.iterateWithTermination(maxIterations) {
      pagerank =>
        val nextPagerank = pagerank.join(adjacencyMatrix).where(_.node).equalTo(_.node)
          .flatMap {
          _ match{
            case (Pagerank(node, rank), AdjacencyRow(_, neighbours)) =>{
              val length = neighbours.length
              (neighbours map {
                Pagerank(_, dampingFactor*rank/length)
              }) :+ Pagerank(node, (1-dampingFactor)/numVertices)
            }
          }
        }.groupBy(_.node).reduce(_ + _)

        val termination = pagerank.join(nextPagerank).where(_.node).equalTo(_.node).flatMap{
          _ match {
            case (Pagerank(_, left), Pagerank(_, right)) => if(left != right) {
              Some(left)
            }else{
              None
            }
          }
        }

        (nextPagerank, termination)
    }

    adjacencyMatrix.print()

    solution.print()

    env.execute("Flink Scala API Skeleton")
  }

  def getInitialPagerank(numVertices: Int, env: ExecutionEnvironment): DataSet[Pagerank] = {
    env.fromCollection(1 to numVertices map { i => Pagerank(i, 1.0d/numVertices)})
  }

  def getInitialAdjacencyMatrix(numVertices: Int, env: ExecutionEnvironment):
  DataSet[AdjacencyRow] = {
    env.fromCollection(1 to numVertices).map{
      node =>
        val rand = new Random(node)
        val neighbours = for(i <- 1 to numVertices if(i != node && rand.nextDouble() > sparsity))
        yield {
          i
        }

        AdjacencyRow(node, neighbours.toArray)
    }
  }
}
