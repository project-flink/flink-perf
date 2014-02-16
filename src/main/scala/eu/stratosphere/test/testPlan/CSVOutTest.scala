package eu.stratosphere.test.testPlan

import eu.stratosphere.api.common.Program
import eu.stratosphere.api.common.ProgramDescription
import eu.stratosphere.client.LocalExecutor
import eu.stratosphere.api.scala.TextFile
import eu.stratosphere.api.scala.ScalaPlan
import eu.stratosphere.api.scala._
import eu.stratosphere.api.scala.operators._
import eu.stratosphere.client.RemoteExecutor

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


/**
 * This is a outline for a Stratosphere scala job. It is actually the WordCount 
 * example from the stratosphere distribution.
 * 
 * You can run it out of your IDE using the main() method of RunJob.
 * This will use the LocalExecutor to start a little Stratosphere instance
 * out of your IDE.
 * 
 * You can also generate a .jar file that you can submit on your Stratosphere
 * cluster.
 * Just type 
 *      mvn clean package
 * in the projects root directory.
 * You will find the jar in 
 *      target/stratosphere-quickstart-0.1-SNAPSHOT-Sample.jar
 *
 */
class CSVOutTest extends Program with ProgramDescription with Serializable {
  override def getDescription() = {
    "Parameters: [input] [output] [numSubStasks]"
  }
  override def getPlan(args: String*) = {
    getScalaPlan(args(1), args(2), args(0).toInt)
  }

  def formatOutput = (word: String, count: Int) => "%s %d".format(word, count)

  def getScalaPlan(textInput: String, wordsOutput: String, numSubTasks: Int) = {
    val input = TextFile(textInput)

    val words = input flatMap { _.toLowerCase().split("""\W+""") filter { _ != "" } map { (_, 1) } }

    val output = words.write(wordsOutput, CsvOutputFormat("\n",",")) 
  
    val plan = new ScalaPlan(Seq(output), "Word Count (immutable)")
    plan.setDefaultParallelism(numSubTasks)
    plan
  }
}