package com.github.projectflink.testPlan

import org.apache.flink.api.common.Program
import org.apache.flink.api.common.ProgramDescription
import org.apache.flink.client.LocalExecutor
import org.apache.flink.api.scala.TextFile
import org.apache.flink.api.scala.ScalaPlan
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.operators._
import org.apache.flink.client.RemoteExecutor

// You can run this locally using:
// mvn exec:exec -Dexec.executable="java" -Dexec.args="-cp %classpath ${package}.RunJobLocal 2 file:///some/path file:///some/other/path"
object RunJobLocal {
  def main(args: Array[String]) {
    val job = new CSVOutTest
    val plan = job.getScalaPlan("file:///tmp/word", "file:///tmp/out", 15)
    LocalExecutor.execute(plan)
    System.exit(0)
  }
}


class CSVOutTest extends Program with ProgramDescription with Serializable {
  override def getDescription() = {
    "Parameters: [input] [output] [numSubStasks]"
  }
  override def getPlan(args: String*) = {
    getScalaPlan(args(0), args(1), args(2).toInt)
  }

  def formatOutput = (word: String, count: Int) => "%s %d".format(word, count)

  def getScalaPlan(textInput: String, wordsOutput: String, numSubTasks: Int) = {
    val input = TextFile(textInput)

    val words = input flatMap { _.toLowerCase().split("""\W+""") filter { _ != "" } map { (_, 1) } }

    val output = words.write(wordsOutput, CsvOutputFormat("\n",",")) 
  
    val plan = new ScalaPlan(Seq(output), "CsvOutTest")
    plan.setDefaultParallelism(numSubTasks)
    plan
  }
}