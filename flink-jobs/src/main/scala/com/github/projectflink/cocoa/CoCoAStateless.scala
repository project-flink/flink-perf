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

import org.apache.flink.api.common.functions.{RichMapFunction, RichJoinFunction, Partitioner}
import org.apache.flink.api.scala.{ExecutionEnvironment, DataSet}
import org.apache.flink.configuration.Configuration
import org.jblas.DoubleMatrix

import org.apache.flink.api.scala._

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class CoCoAStateless(n: Int, numBlocks: Int, outerIterations: Int, innerIterations: Int,
            lambda: Double, scalingParameter: Double, seed: Long) extends Serializable {
  import CoCoAStateless._

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
    }.withForwardedFields("0")

    val initialAW = new RichMapFunction[Datapoints, (Int, Array[Element], Array[Element])] {
      var w: Array[Element] = _

      override def open(parameters: Configuration): Unit = {
        w = getRuntimeContext.getBroadcastVariable(WEIGHT_VECTOR).get(0)
      }

      override def map(datapoints: Datapoints): (Int, Array[Element], Array[Element]) = {
          (datapoints.index, Array.fill(datapoints.data.length)(0.0), w)
      }
    }

    val initialAlphaWUpdates = blockedInput.map(initialAW).
      withBroadcastSet(initialW, WEIGHT_VECTOR).withForwardedFields("0")

    // calculate the weight vector
    val finalAlphaWUpdates = initialAlphaWUpdates.iterate(outerIterations) {
      alphaWUpdates =>
        val wUpdates = alphaWUpdates.map(_._3)
        val w = wUpdates.reduce(_.add(_))
        val alphas = alphaWUpdates.map{
          x => (x._1, x._2)
        }.withForwardedFields("0")

        localDualMethod(w, alphas, blockedInput)
    }

    val wUpdates = finalAlphaWUpdates.map(_._3)
    wUpdates.reduce(_.add(_))
  }


  def localDualMethod(w: DataSet[Array[Element]], alphas: DataSet[(Int, Array[Element])],
                      blockedInput: DataSet[Datapoints]):
  DataSet[(Int, Array[Element], Array[Element])] = {
    val localSDCA = new RichJoinFunction[(Int, Array[Element]), Datapoints, (Int, Array[Element], Array[Element])] {

      var originalW: Array[Element] = _
      var r: Random = _

      override def open(parameters: Configuration): Unit = {
        originalW = getRuntimeContext.getBroadcastVariable(WEIGHT_VECTOR).get(0)

        if(r == null){
          r = new Random(seed ^ getRuntimeContext.getIndexOfThisSubtask)
        }
      }

      override def join(idxAlphas: (Int, Array[Element]), datapoints: Datapoints):
      (Int, Array[Element], Array[Element]) = {
        val (blockIdx, alphas) = idxAlphas

        val numLocalDatapoints = alphas.length
        val newAlphas = alphas.clone()
        val w = originalW.clone()
        val newW = originalW.mul(1.0/numBlocks)

        for(i <- 1 to innerIterations) {
          val idx = r.nextInt(numLocalDatapoints)

          val datapoint = datapoints.data(idx)
          val label = datapoints.labels(idx)
          val alpha = alphas(idx)

          val (deltaAlpha, deltaWUpdate) = maximize(datapoint, label, lambda, alpha, w)

          alphas(idx) += deltaAlpha

          newAlphas(idx) += deltaAlpha*scaling

          w.addi(deltaWUpdate)
          newW.addi(deltaWUpdate.mul(scaling))
        }

        (blockIdx, newAlphas, newW)
      }
    }

    alphas.join(blockedInput).where(0).equalTo(0).apply(localSDCA).
      withBroadcastSet(w, WEIGHT_VECTOR).withForwardedFieldsFirst("0").withForwardedFieldsSecond("0")
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


object CoCoAStateless{
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

    val cocoa = new CoCoAStateless(numberDatapoints, numBlocks, 10, 10, 1.0, 1.0, seed)

    val finalW = cocoa.calculate(toyDataDS, initialWDS)

    finalW.map{
      w =>
        "(" + w.mkString(", ") + ")"
    }.print()

    env.execute("CoCoAStateless")
//    println(env.getExecutionPlan())
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
