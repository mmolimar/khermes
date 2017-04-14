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

package com.stratio.khermes.utils

import java.io.File
import java.util.Properties

import com.stratio.khermes.persistence.clients.{Client, ClientFactory, KafkaClient, MqttClient}
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import io.moquette.BrokerConstants
import io.moquette.server.{Server => MqttServer}
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.{SystemTime, TestUtils}
import org.apache.curator.test.TestingServer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.protocol.SecurityProtocol
import org.junit.rules.TemporaryFolder

import scala.util.Try

trait EmbeddedServersUtils extends LazyLogging {
  type TopicName = String
  val zookeeperConnectString = "127.0.0.1:2181"
  val tmpFolder = new TemporaryFolder()
  tmpFolder.create()
  val logDir = tmpFolder.newFolder("kafkatest")
  val loggingEnabled = true

  def withEmbeddedKafkaServer()(function: KafkaServer => Any): Unit = {
    withEmbeddedZookeeper() { zookeeperServer =>
      zookeeperServer.start
      val kafkaConfig = new KafkaConfig(kafkaConfiguration(logDir, zookeeperServer.getConnectString), loggingEnabled)

      logger.debug("Starting embedded Kafka broker (with log.dirs={} and ZK ensemble at {}) ...",
        logDir, zookeeperConnectString)

      val kafkaServer = TestUtils.createServer(kafkaConfig, SystemTime)
      kafkaServer.startup
      val brokerList =
        s"""${kafkaServer.config.hostName}:${
          Integer.toString(kafkaServer.boundPort(SecurityProtocol.PLAINTEXT))
        }"""

      logger.debug("Startup of embedded Kafka broker at {} completed (with ZK ensemble at {}) ...",
        brokerList, zookeeperConnectString)

      function(kafkaServer)
      kafkaServer.shutdown
      zookeeperServer.stop
    }
  }

  def withEmbeddedMqttServer()(function: MqttServer => Any): Unit = {
    val mqttServer = new MqttServer
    val config = mqttConfiguration
    mqttServer.startServer(config)

    logger.debug("Startup of embedded MQTT broker at port {}",
      config.getProperty(BrokerConstants.PORT_PROPERTY_NAME))

    function(mqttServer)
    mqttServer.stopServer
  }


  def withEmbeddedZookeeper()(function: TestingServer => Any): Unit = {
    function(new TestingServer())
  }

  def withKafkaProducer[V](kafkaServer: KafkaServer)(testFunction: KafkaProducer[String, V] => Any): Unit = {
    val props = kafkaServer.config.originals
    val producer: KafkaProducer[String, V] = new KafkaProducer(props)
    testFunction(producer)
  }

  def withKafkaClient[V <: Object](kafkaServer: KafkaServer, topic: String)(function: KafkaClient[V] => Any): Unit = {
    val props = kafkaServer.config.originals
    props.put("topic", topic)
    val kafkaClient = KafkaClient[V](ConfigFactory.parseMap(props))
    function(kafkaClient)
  }

  def withMqttClient[V <: Object](mqttServer: MqttServer, topic: String)(function: MqttClient[V] => Any): Unit = {
    import scala.collection.JavaConverters._
    val config = Map("topic" -> topic)
    val mqttClient = MqttClient[V](ConfigFactory.parseMap(config.asJava))
    function(mqttClient)
  }

  def withClientsWrapped[V <: Object](kafkaServer: KafkaServer,
                                      mqttServer: MqttServer, topic: String)
                                     (function: Client[Object, Seq[Any]] => Any): Unit = {
    val kafkaConfig = kafkaServer.config.originals
    kafkaConfig.put("topic", topic)
    import scala.collection.JavaConverters._
    val mqttUri = "tcp://" + mqttConfiguration.getProperty(BrokerConstants.HOST_PROPERTY_NAME)  + ":" +
      mqttConfiguration.getProperty(BrokerConstants.HOST_PROPERTY_NAME)
    val mqttConfig = Map("topic" -> topic, "uri" -> mqttUri).asJava
    val clientConfig = Map("kafka" -> kafkaConfig, "mqtt" -> mqttConfig)

    val clientWrapper = ClientFactory.fromConfig(ConfigFactory.parseMap(clientConfig.asJava))
    function(clientWrapper)
  }

  //TODO: Accept initial config parameter (specific traits)
  private def kafkaConfiguration(logDir: File, zkConnectString: String) = {
    val kafkaConfig = new Properties()
    kafkaConfig.put(KafkaConfig.ZkConnectProp, zkConnectString)
    kafkaConfig.put(KafkaConfig.BrokerIdProp, "0")
    kafkaConfig.put(KafkaConfig.HostNameProp, "127.0.0.1")
    kafkaConfig.put(KafkaConfig.PortProp, "9092")
    kafkaConfig.put(KafkaConfig.NumPartitionsProp, "1")
    kafkaConfig.put(KafkaConfig.AutoCreateTopicsEnableProp, "true")
    kafkaConfig.put(KafkaConfig.MessageMaxBytesProp, "1000000")
    kafkaConfig.put(KafkaConfig.ControlledShutdownEnableProp, "true")
    kafkaConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
    kafkaConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    kafkaConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    kafkaConfig.setProperty(KafkaConfig.LogDirProp, logDir.getAbsolutePath)
    //effectiveConfig.putAll(initialConfig);
    kafkaConfig
  }

  private def mqttConfiguration = {
    new Properties {
      setProperty(BrokerConstants.HOST_PROPERTY_NAME, BrokerConstants.HOST)
      setProperty(BrokerConstants.PORT_PROPERTY_NAME, BrokerConstants.PORT.toString)
    }
  }
}
