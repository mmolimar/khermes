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

import java.util.UUID

import com.stratio.khermes.utils.EmbeddedServersUtils
import com.typesafe.scalalogging.LazyLogging
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

import scala.util.Try

@RunWith(classOf[JUnitRunner])
class ClientsTest extends FlatSpec
  with Matchers
  with LazyLogging
  with EmbeddedServersUtils {

  val Message = "testMessage"
  val Topic = s"topic-${UUID.randomUUID().toString}"
  val NumberOfMessages = 3

  "A KafkaClient" should "initialize properly" in {
    withEmbeddedKafkaServer() { kafkaServer =>
      withKafkaClient[Object](kafkaServer, Topic) { client =>
        Option(client.parseProperties(client.config).getProperty("bootstrap.servers")) should not be (None)
        client should be equals "kafka"
        client.topic should be equals (Topic)
      }
    }
  }

  it should "produce messages in a topic" in {
    withEmbeddedKafkaServer() { kafkaServer =>
      withKafkaClient[Object](kafkaServer, Topic)(_ => () => produce(_))
    }
  }

  "A MqttClient" should "initialize properly" in {
    withEmbeddedMqttServer() { mqttServer =>
      withMqttClient[Object](mqttServer, Topic) { client =>
        client should be equals "mqtt"
        client.qos should be(1)
        client.retained should be(false)
        client.topic should be equals (Topic)
      }
    }
  }

  it should "produce messages in a topic" in {
    withEmbeddedKafkaServer() { kafkaServer =>
      withKafkaClient[Object](kafkaServer, Topic)(_ => () => produce(_))
    }
    withEmbeddedMqttServer() { mqttServer =>
      withMqttClient[Object](mqttServer, Topic)(_ => () => produce(_))
    }
  }

  "A Client wrapper" should "produce messages in Kafka and MQ" in {
    withEmbeddedKafkaServer() { kafkaServer =>
      withEmbeddedMqttServer() { mqttServer =>
        withClientsWrapped[Object](kafkaServer, mqttServer, Topic) { client =>
          val clientNames = client.name.split(",").toSet
          clientNames should contain("kafka")
          clientNames should contain("mqtt")
          produce(client.asInstanceOf[Client[Object, Any]]).isSuccess should be(true)
        }
      }
    }
  }

  private def produce(client: Client[Object, Any]): Try[Any] = {
    Try {
      (1 to NumberOfMessages).foreach(_ => {
        client.send(Message)
      })
      client.close
    }
  }
}