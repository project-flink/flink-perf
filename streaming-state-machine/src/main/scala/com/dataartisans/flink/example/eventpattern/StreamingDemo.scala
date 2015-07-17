/*
 * Copyright 2015 Data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.flink.example.eventpattern

import java.util.Properties

import _root_.kafka.consumer.ConsumerConfig
import com.dataartisans.flink.example.eventpattern.kafka.EventDeSerializer

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.checkpoint.Checkpointed
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.Utils.TypeInformationSerializationSchema
import org.apache.flink.streaming.connectors.kafka.api.KafkaSink
import org.apache.flink.streaming.connectors.kafka.api.persistent.PersistentKafkaSource
import org.apache.flink.util.Collector
import org.slf4j.{LoggerFactory, Logger}

import scala.collection.mutable

/**
 * Demo streaming program that receives (or generates) a stream of events and evaluates
 * a state machine (per originating IP address) to validate that the events follow
 * the state machine's rules.
 */
object StreamingDemo {


  def main(args: Array[String]): Unit = {

    val pt = ParameterTool.fromArgs(args)
    // create the environment to create streams and configure execution
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    if(pt.has("par")) {
      env.setParallelism(pt.getInt("par"))
    }

    if(pt.has("retries")) {
      env.setNumberOfExecutionRetries(pt.getInt("retries"))
    }
    
    // this statement enables the checkpointing mechanism with an interval of 1 sec
    env.enableCheckpointing(pt.getInt("ft", 1000))

    
    // data stream from kafka topic.
    val props = new Properties()
    props.put("group.id", "flink-streaming-demo")
    props.put("auto.commit.enable", "false")
    props.putAll(pt.toMap)

    val stream = env.addSource(new PersistentKafkaSource[Event](pt.getRequired("topic"),
                                                                new EventDeSerializer(),
                                                                new ConsumerConfig(props)))

    stream
      // partition on the address to make sure equal addresses
      // end up in the same state machine flatMap function
      .partitionByHash("sourceAddress")
      
      // the function that evaluates the state machine over the sequence of events
      .flatMap(new StateMachineMapper(pt))
      
      // output to standard-out
    //  .addSink(new KafkaSink(pt.getRequired("brokerList"), pt.getRequired("error-topic"), props, new TypeInformationSerializationSchema[String]("", env.getConfig)))
    
    // trigger program execution
    env.execute()
  }
}

/**
 * The function that maintains the per-IP-address state machines and verifies that the
 * events are consistent with the current state of the state machine. If the event is not
 * consistent with the current state, the function produces an alert.
 */
class StateMachineMapper(val pt: ParameterTool) extends FlatMapFunction[Event, String] with Checkpointed[mutable.HashMap[Int, State]] {

  private val LOG: Logger = LoggerFactory.getLogger(classOf[StateMachineMapper])
  
  private[this] val states = new mutable.HashMap[Int, State]()

  var received = 0L
  var lastLog = -1L
  var lastElements = 0L
  val logfreq = pt.getInt("logFreq")
  
  override def flatMap(t: Event, out: Collector[String]): Unit = {

    // get and remove the current state
    val state = states.remove(t.sourceAddress).getOrElse(InitialState)
    
    val nextState = state.transition(t.event)
    if (nextState == InvalidTransition) {
      val al = Alert(t.sourceAddress, state, t.event).toString
      LOG.info("Detected invalid state transition {}", al)
      out.collect(al)
    } 
    else if (!nextState.terminal) {
      states.put(t.sourceAddress, nextState)
    }

    // some statistics
    received += 1
    if (received % logfreq == 0) {
      val now: Long = System.currentTimeMillis

      if (lastLog == -1) {
        lastLog = now
        lastElements = received
      }
      else {
        val timeDiff: Long = now - lastLog
        val elementDiff: Long = received - lastElements
        val ex: Double = 1000 / timeDiff.toDouble
        LOG.info(s"During the last $timeDiff ms, we received $elementDiff elements. That's {} elements/second/core", elementDiff * ex)
        lastLog = now
        lastElements = received
      }
    }
  }

  /**
   * Draws a snapshot of the function's state.
   * 
   * @param checkpointId The ID of the checkpoint.
   * @param timestamp The timestamp when the checkpoint was instantiated.
   * @return The state to be snapshotted, here the hash map of state machines.
   */
  override def snapshotState(checkpointId: Long, timestamp: Long): mutable.HashMap[Int, State] = {
    states
  }

  /**
   * Restores the state.
   * 
   * @param state The state to be restored.
   */
  override def restoreState(state: mutable.HashMap[Int, State]): Unit = {
    states ++= state
  }
}
