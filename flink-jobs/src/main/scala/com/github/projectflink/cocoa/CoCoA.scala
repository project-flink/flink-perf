/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.projectflink.cocoa

import org.apache.flink.api.common.functions.{RichMapFunction, Partitioner}
import org.apache.flink.api.scala.{ExecutionEnvironment, DataSet}
import org.apache.flink.configuration.Configuration
import org.jblas.DoubleMatrix

import org.apache.flink.api.scala._

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class CoCoA(n: Int, numBlocks: Int, outerIterations: Int, innerIterations: Int,
            lambda: Double, scalingParameter: Double, seed: Long) extends Serializable {
  import CoCoA._

  val scaling = scalingParameter/numBlocks


  def calculate(input: DataSet[Datapoint], initialW: DataSet[Array[Element]]): DataSet[Array[Element]] = {
    val partitioner = new BlockPartitioner()

    // first group the input data into blocks
    val blockedInput = input.map {
      datapoint =>
        (datapoint.index % numBlocks, datapoint)
    }.partitionCustom(partitioner, 0).groupBy(0).reduceGroup{
      datapoints =>
        var blockIndex = -1
        val data = ArrayBuffer[Array[Element]]()
        val labels = ArrayBuffer[Element]()
        var dimension = -1

        while(datapoints.hasNext){
          val (index, datapoint) = datapoints.next()

          blockIndex = index % numBlocks
          dimension = datapoint.data.length

          data += datapoint.data
          labels += datapoint.label
        }

        Datapoints(blockIndex, data.toArray, labels.toArray)
    }

    // calculate the weight vector
    initialW.iterate(outerIterations) {
      w =>
        val deltaWs = localDualMethod(w, blockedInput)
        val weightedDeltaWs = deltaWs map {
          _.muli(scaling).data
        }

        w.union(weightedDeltaWs).reduce{
          _.add(_)
        }
    }
  }


  def localDualMethod(w: DataSet[Array[Element]], blockedInput: DataSet[Datapoints]):
  DataSet[Array[Element]] = {
    val localSDCA = new RichMapFunction[Datapoints, Array[Element]] {
      var originalW: Array[Element] = _
      val alphasArray = ArrayBuffer[Array[Element]]()
      val idMapping = scala.collection.mutable.HashMap[Int, Int]()
      var counter = 0

      var r: Random = _

      override def open(parameters: Configuration): Unit = {
        originalW = getRuntimeContext.getBroadcastVariable(WEIGHT_VECTOR).get(0)

        if(r == null){
          r = new Random(seed ^ getRuntimeContext.getIndexOfThisSubtask)
        }
      }

      override def map(in: Datapoints): Array[Element] = {
        val localIndex = idMapping.get(in.index) match {
          case Some(idx) => idx
          case None =>
            idMapping += (in.index -> counter)
            counter += 1

            alphasArray += Array.fill(in.labels.length)(0.0)

            counter - 1
        }

        val alphas = alphasArray(localIndex).clone()
        val numLocalDatapoints = alphas.length
        val deltaAlphas = Array.fill(numLocalDatapoints)(0.0)
        val w = originalW.clone()
        val deltaW = Array.fill(originalW.length)(0.0)

        for(i <- 1 to innerIterations) {
          val idx = r.nextInt(numLocalDatapoints)

          val datapoint = in.data(idx)
          val label = in.labels(idx)
          val alpha = alphas(idx)

          val (deltaAlpha, deltaWUpdate) = maximize(datapoint, label, lambda, alpha, w)

          alphas(idx) += deltaAlpha

          deltaAlphas(idx) += deltaAlpha

          w.addi(deltaWUpdate)
          deltaW.addi(deltaWUpdate)
        }

        // update local alpha values
        alphasArray(localIndex) = (alphasArray(localIndex)).add(deltaAlphas.mul(scaling))

        deltaW
      }
    }

    blockedInput.map(localSDCA).withBroadcastSet(w, WEIGHT_VECTOR)
  }

  def maximize(x: Array[Element], y: Element, lambda: Double, alpha: Double,
               w: Array[Element]): (Double, Array[Element]) = {
    // compute hinge loss gradient
    val grad = (y*(x.dot(w)) - 1.0)*(lambda*n)

    // compute projected gradient
    var proj_grad = if(alpha  <= 0.0){
      Math.min(grad, 0)
    } else if(alpha >= 1.0) {
      Math.max(grad, 0)
    } else {
      grad
    }

    if(Math.abs(grad) != 0.0){
      val qii = x.dot(x)
      val newAlpha = if(qii != 0.0){
        Math.min(Math.max((alpha - (grad / qii)), 0.0), 1.0)
      } else {
        1.0
      }

      val deltaW = x.mul( y*(newAlpha-alpha)/(lambda*n) )

      (newAlpha - alpha, deltaW)
    } else {
      (0.0 , Array.fill(w.length)(0.0))
    }
  }


  class BlockPartitioner extends Partitioner[Int] {
    override def partition(key: Int, i: Int): Int = {
      key % i
    }
  }
}


object CoCoA{
  val WEIGHT_VECTOR ="weightVector"
  type Element = Double

  case class Datapoint(index: Int, data: Array[Element], label: Element)
  case class Datapoints(index: Int, data: Array[Array[Element]], labels: Array[Element])


  implicit def array2JBlasMatrix(array: Array[Element]): DoubleMatrix = {
    new DoubleMatrix(array)
  }

  implicit def jblasMatrix2Array(matrix: DoubleMatrix): Array[Double] = {
    matrix.data
  }

  def main(args: Array[String]): Unit = {
    val numBlocks = 10
    val toyData = getToyData
    val numberDatapoints = toyData.length
    val dimension = toyData(0).data.length
    val initialW = Array.fill(dimension)(0.0)
    val seed = System.currentTimeMillis()

    val env = ExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(numBlocks)

    val toyDataDS = env.fromCollection(toyData)
    val initialWDS = env.fromElements(initialW)

    val cocoa = new CoCoA(numberDatapoints, numBlocks, 10, 10, 1.0, 1.0, seed)

    val finalW = cocoa.calculate(toyDataDS, initialWDS)

    finalW.map{
      w =>
        "(" + w.mkString(", ") + ")"
    }.print()

    env.execute("CoCoA")
  }

  def getToyData: List[Datapoint] = {
    List(
      Datapoint(0, Array(1,4), 1),
      Datapoint(1, Array(2,1), 1),
      Datapoint(2, Array(3, 6), -1),
      Datapoint(3, Array(1,3), 1),
      Datapoint(4, Array(5,4), -1),
      Datapoint(5, Array(6,9), 1),
      Datapoint(6, Array(5,2), -1),
      Datapoint(7, Array(1,7), -1),
      Datapoint(8, Array(3,7), 1),
      Datapoint(9, Array(8,5), -1)
    )
  }
}
