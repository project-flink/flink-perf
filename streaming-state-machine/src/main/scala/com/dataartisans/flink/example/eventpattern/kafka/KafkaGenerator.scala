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

//package com.dataartisans.flink.example.eventpattern.kafka
//
//import java.util.Properties
//
//import com.dataartisans.flink.example.eventpattern.{StandaloneGeneratorBase, Event}
//import org.apache.flink.api.java.utils.ParameterTool
//import org.apache.flink.util.Collector
//import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
//import org.apache.kafka.common.serialization.ByteArraySerializer
//
///**
// * A generator that pushes the data into Kafka.
// */
//object KafkaGenerator extends StandaloneGeneratorBase {
//
//
//  def main(args: Array[String]): Unit = {
//    val pt = ParameterTool.fromArgs(args)
//
//    val numPartitions = pt.getInt("threads")
//    val collectors = new Array[KafkaCollector](numPartitions)
//
//    // create the generator threads
//    for (i <- 0 until collectors.length) {
//      collectors(i) = new KafkaCollector(i, pt)
//    }
//
//    runGenerator(collectors)
//  }
//}
//class AllToOne(props: VerifiableProperties) extends kafka.producer.Partitioner {
//  override def partition(key: Any, numPartitions: Int): Int = 68
//}
//
//class KafkaCollector(private[this] val partition: Int, private val pt: ParameterTool) extends Collector[Event] {
//
//  // create Kafka producer
//  val properties = new Properties()
//  properties.put("bootstrap.servers", "localhost:9092")
//  properties.put("value.serializer", classOf[ByteArraySerializer].getCanonicalName)
//  properties.put("key.serializer", classOf[ByteArraySerializer].getCanonicalName)
// // properties.put("partitioner.class", classOf[AllToOne].getCanonicalName)
//
//  properties.putAll(pt.toMap)
//
// // val config: ProducerConfig = new ProducerConfig()
//
//  val producer = new KafkaProducer[Event, Array[Byte]](properties)
//
//  val serializer = new EventDeSerializer()
//  val topic = pt.getRequired("topic")
//
//  var serLen = 0L
//  override def collect(t: Event): Unit = {
//    val serialized = serializer.serialize(t)
//    serLen = serLen + serialized.length
//
//   // producer.send(new ProducerRecord[Event, Array[Byte]](topic, null, serialized))
//  }
//
//  override def close(): Unit = {
//    producer.close()
//  }
//}
