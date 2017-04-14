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

import java.io.Closeable

import com.typesafe.config.{Config, ConfigUtil}
import com.typesafe.scalalogging.LazyLogging

import scala.collection.JavaConverters._


object AvailableClients {
  val Kafka = "kafka"
  val Mqtt = "mqtt"
  val all = Seq(Kafka, Mqtt)
}

trait Client[K <: Object, V <: Any] extends Closeable with LazyLogging {

  def name: String

  def send(message: K): V
}

object ClientFactory extends LazyLogging {

  private def makeClient(name: String, config: Config): Client[Object, Any] = {
    {
      name match {
        case AvailableClients.Kafka => KafkaClient(config)
        case AvailableClients.Mqtt => MqttClient(config)
        case unknown => throw new IllegalArgumentException(s"Client $unknown is not a valid config")
      }
    }
  }.asInstanceOf[Client[Object, Any]]

  def fromConfig(config: Config): Client[Object, Seq[Any]] = {
    val clients = config.entrySet.asScala.map { entry =>
      ConfigUtil.splitPath(entry.getKey).get(0)
    }.map { key =>
      makeClient(key, config.getObject(key).toConfig)
    }.toSeq

    new ClientWrapper(clients)
  }
}

private[clients] class ClientWrapper(private val underlying: Seq[Client[Object, Any]]) extends Client[Object, Seq[Any]] {

  require(!underlying.isEmpty, "Client list must not be empty")

  override def name: String = underlying.map(_.name).mkString(",")

  override def send(message: Object): Seq[Any] = underlying.map(_.send(message))

  override def close(): Unit = underlying.foreach(_.close)
}
