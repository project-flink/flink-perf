package com.github.projectflink

import org.apache.flink.api.java.io.CsvInputFormat
import org.apache.flink.api.java.typeutils.TypeInfoParser
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.operators.ScalaCsvInputFormat
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

    //val csvIn = new CsvInputFormat[org.apache.flink.api.java.tuple.Tuple1[String]](new Path(inPath), classOf[String])
    //csvIn.setSkipFirstLineAsHeader(true);
   // val inData : DataSet[org.apache.flink.api.java.tuple.Tuple1[String]] = env.createInput(csvIn)
  //  val csvIn = new ScalaCsvInputFormat[Tuple2[Int, Int]](new Path(inPath), fakeType.getType() );
    val inData = env.readTextFile(inPath)
    val pairs = inData.map({ line =>
      val sp = line.split(" ")
      (sp(0).toInt, sp(1).toInt) // return tuple (int, int)
    })

    val grouped = pairs.groupBy(0);
    val adjacencyMatrix : DataSet[AdjacencyRow] = grouped.reduceGroup {
      group =>
        val iterable = group.toIterable
        val head = iterable.head
        val result = iterable.foldLeft((head._1, List(head._2))){
          case ((id, neighbours), (_, neighbour)) => (id, neighbour :: neighbours)
        }
        AdjacencyRow(result._1, result._2.toArray)
    }

    val initialPagerank : DataSet[Pagerank] = {
      //env.fromCollection(1 to numVertices) map { Pagerank(_, 1.0d/numVertices)}
      env.generateSequence(1, numVertices) map{ t => Pagerank(t.toInt, 1.0d/numVertices)}
    }
    //val adjacencyMatrix = getInitialAdjacencyMatrix(numVertices, env)
    //val initialPagerank = getInitialPagerank(numVertices, env)

    val solution = initialPagerank.iterate(maxIterations) {
        _.join(adjacencyMatrix).where(_.node).equalTo(_.node)
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
    }

    //adjacencyMatrix.print()

    // solution.print()
    solution.writeAsText(outPath+"_flink", WriteMode.OVERWRITE)

    env.execute("Page Rank Flink Scala")
  }

/*  def getInitialPagerank(numVertices: Int, env: ExecutionEnvironment): DataSet[Pagerank] = {
    env.fromCollection(1 to numVertices map { i => Pagerank(i, 1.0d/numVertices)})
  }

  def getInitialAdjacencyMatrix(numVertices: Int, env: ExecutionEnvironment):
  DataSet[AdjacencyRow] = {
    env.fromCollection(1 to numVertices).map{
      node =>
        val rand = new Random(node)
        val neighbours = for(i <- 1 to numVertices if(i != node && rand.nextDouble() > 0.5)) yield {

          i
        }

        AdjacencyRow(node, neighbours.toArray)
    }
  } */
}