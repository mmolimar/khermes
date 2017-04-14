/*
 * Copyright (C) 2016 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stratio.khermes.persistence.clients

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.util.Properties
import java.util.concurrent.{Future, TimeUnit}

import com.stratio.khermes.commons.implicits.AppImplicits._
import com.typesafe.config.Config
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import org.eclipse.paho.client.mqttv3.{IMqttDeliveryToken, MqttConnectOptions, MqttMessage, MqttAsyncClient => PahoMqttClient}

import scala.annotation.tailrec
import scala.collection.concurrent.{Map => ConcurrentMap}
import scala.util.Random

abstract class BaseClient[K <: Object, V <: Any] extends Client[K, V] {

  val clientId = getClientId

  implicit def toByteArray[T](obj: T): Array[Byte] = {
    def serialize(obj: Any): Array[Byte] = {
      val baos = new ByteArrayOutputStream
      val oos = new ObjectOutputStream(baos)
      oos.writeObject(obj)
      oos.close
      baos.toByteArray
    }

    obj match {
      case s: String => s.getBytes
      case a: Array[Byte] => obj.asInstanceOf[Array[Byte]]
      case _ => serialize(obj)
    }
  }

  final def parseProperties(config: Config): Properties = {
    import scala.collection.JavaConversions._
    val props = new Properties
    val map: Map[String, Object] = config.entrySet().map({
      entry =>
        entry.getKey -> entry.getValue.unwrapped()
    })(collection.breakOut)
    props.putAll(map)
    props
  }

  @tailrec
  // scalastyle:off magic.number
  final protected def waitUntil(condition: () => Boolean, timeout: Int, timeUnit: TimeUnit = TimeUnit.MILLISECONDS): Boolean = {
    if (condition()) {
      true
    } else {
      Thread.sleep(50)
      if (timeout == 0) false else waitUntil(condition, timeout - 50)
    }
  }

  protected def getClientId: String
}

case class KafkaClient[K <: Object](config: Config)
  extends BaseClient[K, Future[RecordMetadata]] {

  val topic: String = config.getString("topic")

  lazy val producer: KafkaProducer[String, Array[Byte]] = {
    val props = parseProperties(config)
    props.put("client.id", clientId)
    val kp = new KafkaProducer[String, Array[Byte]](props)

    logger.info("Kafka Producer initialized")
    kp
  }

  override def name: String = AvailableClients.Kafka

  implicit def kafkaMessage(message: K): ProducerRecord[String, Array[Byte]] = {
    new ProducerRecord[String, Array[Byte]](topic, message)
  }

  override def send(message: K): Future[RecordMetadata] = producer.send(message)

  // scalastyle:off magic.number
  override def getClientId: String = config.getOrElse("client-id", "khermes-" + Random.alphanumeric.take(10).mkString)

  override def close(): Unit = {
    logger.info(s"Closing producer: $name")
    producer.flush
    producer.close
  }
}

case class MqttClient[K <: Object](config: Config)
  extends BaseClient[K, IMqttDeliveryToken] {

  //at least one
  val qos: Int = config.getOrElse("qos", 1)
  val retained: Boolean = config.getOrElse("retained", false)
  val topic: String = config.getString("topic")

  // highest possible pending delivery tokens
  private val thresold: Int = 65535 - 1

  lazy val producer: PahoMqttClient = {
    val client = new PahoMqttClient(config.getString("uri"), clientId, new MemoryPersistence)

    val options = new MqttConnectOptions {
      // scalastyle:off null
      setUserName(config.getOrElse[String]("username", null))
      setPassword(config.getOrElse[Array[Char]]("password", null))
      // scalastyle:on null
      setAutomaticReconnect(true)
      setMaxInflight(config.getOrElse("max-in-flight-req", 100000))
    }
    client.connect(options)
    waitUntil(() => client.isConnected, 5000)

    logger.info("MQTT Producer initialized")
    client
  }

  implicit def mqttMessage(message: K): MqttMessage = {
    new MqttMessage(message) {
      setQos(qos)
      setRetained(retained)
    }
  }

  override def getClientId: String = config.getOrElse("client-id", PahoMqttClient.generateClientId)

  override def name: String = AvailableClients.Mqtt

  override def send(message: K): IMqttDeliveryToken = {
    //we must wait, if applies, in order to avoid overwhelming the producer
    waitUntil(() => producer.getPendingDeliveryTokens.length < thresold, 5000)
    producer.publish(topic, message)
  }

  override def close(): Unit = {
    logger.info(s"Closing producer: $name")
    producer.disconnect
    producer.close
  }
}
