/**
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
package com.github.projectflink

import java.lang
import java.lang.management.ManagementFactory

import org.apache.flink.api.common.accumulators.LongCounter
import org.apache.flink.api.common.functions.RichMapPartitionFunction
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.util.Collector

import scala.collection.mutable

import scala.collection.JavaConverters._
import scala.util.Random

class MTuple3[T1, T2, T3](var _1: T1, var _2: T2, var _3: T3) {
  def this() = this(null.asInstanceOf[T1], null.asInstanceOf[T2], null.asInstanceOf[T3])

  override def hashCode = 73*_1.hashCode() + 79*_2.hashCode() + _3.hashCode()
  override def equals(other: Any): Boolean = {
    if (!(other.isInstanceOf[MTuple3[_, _, _]])) {
      return false
    }
    val oTuple = other.asInstanceOf[MTuple3[_, _, _]]
    _1.equals(oTuple._1) && _2.equals(oTuple._2) && _3.equals(oTuple._3)
  }

  override def toString = "M(" + _1 + "," + _2 + "," + _3 + ")"
}

class MTuple2[T1, T2](var _1: T1, var _2: T2) {
  def this() = this(null.asInstanceOf[T1], null.asInstanceOf[T2])

  override def hashCode = 73*_1.hashCode() + _2.hashCode()
  override def equals(other: Any): Boolean = {
    if (!(other.isInstanceOf[MTuple2[_, _]])) {
      return false
    }
    val oTuple = other.asInstanceOf[MTuple2[_, _]]
    _1.equals(oTuple._1) && _2.equals(oTuple._2)
  }

  override def toString = "M(" + _1 + "," + _2 + ")"
}

class MTuple2Cheat[T1, T2](var _1: T1, var _2: T2) {
  def this() = this(null.asInstanceOf[T1], null.asInstanceOf[T2])

  override def hashCode = _1.hashCode()
  override def equals(other: Any): Boolean = {
    if (!(other.isInstanceOf[MTuple2Cheat[_, _]])) {
      return false
    }
    val oTuple = other.asInstanceOf[MTuple2Cheat[_, _]]
    _1.equals(oTuple._1)
  }

  override def toString = "M(" + _1 + "," + _2 + ")"
}

object GroupReduceBenchmarkGenerateData {
  val rnd = new Random(System.currentTimeMillis())

  private final def skewedSample(skew: Double, max: Long): Long = {
    val uniform = rnd.nextDouble()
    val `var` = Math.pow(uniform, skew)
    val pareto = 0.2 / `var`
    val `val` = pareto.toInt
    if (`val` > max) `val` % max else `val`
  }

  def main(args: Array[String]) {
    if (args.length < 6) {
      println("Usage: [master] [dop] [numCountries] [numBooks] [numReads] [skew] [output]")
      return
    }

    var master = args(0)
    var dop = args(1).toInt
    var numCountries = args(2).toLong
    var numBooks = args(3).toInt
    var numReads = args(4).toLong
    var skew = args(5).toFloat
    val outputPath = if (args.length > 6) {
      args(6)
    } else {
      null
    }

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(dop)

    val countryIds = env.generateSequence(0, numCountries - 1)
    val bookIds = env.generateSequence(0, numBooks - 1)

    val countryNames = countryIds map { id => (id, "No. " + id + " The awesome country") }
    val bookNames = bookIds map { id => (id, "No. " + id + " This is the most special song") }

    val reads = env.generateSequence(0, numReads - 1) map {
      i =>
        (skewedSample(skew, numCountries - 1), rnd.nextInt(numBooks).toLong)
    }

    val readsWithCountry = reads.joinWithTiny(countryNames).where("_1").equalTo("_1") {
      (left, right) =>
        (right._2, left._2)
    }

    val readsWithCountryAndBook = readsWithCountry.join(bookNames, JoinHint.REPARTITION_HASH_SECOND)
      .where("_2")
      .equalTo("_1") {
      (left, right) =>
        (left._1, right._2)
    }.rebalance()



    if (outputPath == null) {
      readsWithCountryAndBook.print()
    } else {
      readsWithCountryAndBook.writeAsCsv(outputPath + s"$numCountries-$numBooks-$numReads-$skew",
        rowDelimiter = "\n",
        fieldDelimiter = ",",
        writeMode = WriteMode.OVERWRITE)
    }

    // execute program
    env.execute("Group Reduce Benchmark Data Generator")
  }
}

object GroupReduceBenchmarkFlink {

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
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(dop)

    val readsWithCountryAndBook = env.readCsvFile[(String, String)](inputPath)

    // Group by country and determine top-k books
    val result = readsWithCountryAndBook
      .map { in => new MTuple2(new MTuple2(in._1, in._2), 1L) }
      .groupBy("_1.*")
      .sum("_2")
//      .groupBy("_1").sortGroup("_3", Order.DESCENDING)
//      .first(k)
//      .groupBy("_1")
//      .reduceGroup {
//      in =>
//        val it = in.toIterator.buffered
//        val first = it.head
//        (first._1, it.map(in => (in._2, in._3)).mkString(", "))
//    }

    if (outputPath == null) {
      result.print()
    } else {
      result.writeAsCsv(outputPath + "_flink", writeMode = WriteMode.OVERWRITE)
    }

    // execute program
    env.execute("Group Reduce Benchmark Flink")
//    println(env.getExecutionPlan())
  }
}

object GroupReduceBenchmarkFlinkHashCombine {

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
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(dop)

    val readsWithCountryAndBook = env.readCsvFile[(String, String)](inputPath)

    // Group by country and determine top-k books
    val result = readsWithCountryAndBook
      .map { in => new MTuple2Cheat(new MTuple2(in._1, in._2), 1L) }
      .mapPartition(new MutableHashAggregator("COMBINER"))
      .partitionByHash("_1.*")
      .groupBy("_1.*")
      .sum("_2")
//      .groupBy("_1").sortGroup("_3", Order.DESCENDING)
//      .first(k)
//      .groupBy("_1")
//      .reduceGroup {
//      in =>
//        val it = in.toIterator.buffered
//        val first = it.head
//        (first._1, it.map(in => (in._2, in._3)).mkString(", "))
//    }

    if (outputPath == null) {
      result.print()
    } else {
      result.writeAsCsv(outputPath + "_flink", writeMode = WriteMode.OVERWRITE)
    }

    // execute program
    env.execute("Group Reduce Benchmark Flink (HashCombine)")
    //        println(env.getExecutionPlan())
  }
}

object GroupReduceBenchmarkFlinkHash {

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
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(dop)

    val readsWithCountryAndBook = env.readCsvFile[(String, String)](inputPath)
//    val readsWithCountryAndBook = env.fromElements(
//      ("Land 1", "Song 1"),
//      ("Land 1", "Song 1"),
//      ("Land 1", "Song 1"),
//      ("Land 3", "Song 3"),
//      ("Land 3", "Song 3"),
//      ("Land 2", "Song 2"))

    // Group by country and determine top-k books
    val result = readsWithCountryAndBook
      .map { in => new MTuple2Cheat(new MTuple2(in._1, in._2), 1L) }
      .mapPartition(new MutableHashAggregator("COMBINER"))
      .partitionByHash("_1.*")
      .mapPartition(new MutableHashAggregator("REDUCER"))
      .map { in => (in._1._1, in._1._2, in._2) }
//      .groupBy("_1").sortGroup("_3", Order.DESCENDING)
//      .first(k)
//      .groupBy("_1")
//      .reduceGroup {
//      in =>
//        val it = in.toIterator.buffered
//        val first = it.head
//        (first._1, it.map(in => (in._2, in._3)).mkString(", "))
//    }

    if (outputPath == null) {
      result.print()
    } else {
      result.writeAsCsv(outputPath + "_flink", writeMode = WriteMode.OVERWRITE)
    }

    // execute program
    env.execute("Group Reduce Benchmark Flink (Hash)")
//        println(env.getExecutionPlan())
  }
}

class MutableHashAggregator(combine: String)
  extends RichMapPartitionFunction[
    MTuple2Cheat[MTuple2[String, String], Long],
    MTuple2Cheat[MTuple2[String, String], Long]] {

  private var aggregate: mutable.HashMap[
    MTuple2Cheat[MTuple2[String, String], Long],
    MTuple2Cheat[MTuple2[String, String], Long]] = null

  var gcCount = new LongCounter
  var gcTime = new LongCounter

  override def open(config: Configuration) {
    aggregate = mutable.HashMap[
      MTuple2Cheat[MTuple2[String, String], Long],
      MTuple2Cheat[MTuple2[String, String], Long]]()
    val ctx = getRuntimeContext
    ctx.addAccumulator("gc-time-" + ctx.getIndexOfThisSubtask, gcTime)
    ctx.addAccumulator("gc-count-" + ctx.getIndexOfThisSubtask, gcCount)
  }

  override def mapPartition(
      records: lang.Iterable[MTuple2Cheat[MTuple2[String, String], Long]],
      out: Collector[MTuple2Cheat[MTuple2[String, String], Long]]): Unit = {

    val gcBeans = ManagementFactory.getGarbageCollectorMXBeans.asScala
    var beforeGcCount = 0L
    var beforeGcTime = 0L
    gcBeans foreach {
      gc =>
        beforeGcCount += gc.getCollectionCount
        beforeGcTime += gc.getCollectionTime
    }

    val it = records.iterator
    while (it.hasNext) {
      val value = it.next()
      val agg = aggregate.getOrElse(
        value,
        new MTuple2Cheat(new MTuple2(value._1._1, value._1._2), 0L))
      agg._2 += value._2
      aggregate.put(agg, agg)
//      val newKey = new MTuple2(value._1._1, value._1._2)
    }

    var afterGcCount = 0L
    var afterGcTime = 0L
    gcBeans foreach {
      gc =>
        afterGcCount += gc.getCollectionCount
        afterGcTime += gc.getCollectionTime
    }

    gcCount.add(afterGcCount - beforeGcCount)
    gcTime.add(afterGcTime - beforeGcTime)


    val result = new MTuple2Cheat(new MTuple2("", ""), 0L)
    val outIt = aggregate.values.iterator

    while (outIt.hasNext) {
      val value = outIt.next()
      out.collect(value)
    }
  }
}

//class HashGrouper(k: Int)
//  extends RichMapPartitionFunction[(String, String, Long), MTuple2[String, String]] {
//
//  private var groups: mutable.HashMap[String, mutable.MutableList[(String, Long)]] = null
//
//  override def open(config: Configuration) {
//    groups = mutable.HashMap[String, mutable.MutableList[(String, Long)]]()
//  }
//
//  override def mapPartition(
//      records: lang.Iterable[(String, String, Long)],
//      out: Collector[MTuple2[String, String]]): Unit = {
//    val it = records.iterator()
//    while (it.hasNext) {
//      val value = it.next()
//      val list = groups.getOrElse(value._1, mutable.MutableList[(String, Long)]())
//      list += ((value._2, value._3))
//      groups.put(value._1, list)
//    }
//
//    val listIt = groups.iterator
//    while (listIt.hasNext) {
//      val value = listIt.next()
//      val key = value._1
//      val list = value._2
//      val sorted = list.sortBy(- _._2)
////      println("SORTED " + sorted.take(5))
//      out.collect((key, sorted.take(k).mkString(", ") ))
//    }
//  }
//}


