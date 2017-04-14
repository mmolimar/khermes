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

package com.stratio.khermes.cluster.supervisor

import akka.actor.Props
import com.stratio.khermes.cluster.BaseActorTest
import com.stratio.khermes.cluster.supervisor.NodeSupervisorActor.{Start, WorkerStatus}
import com.stratio.khermes.commons.config.AppConfig
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import scala.concurrent.duration._

//@RunWith(classOf[JUnitRunner])
class NodeSupervisorActorTest extends BaseActorTest {

  val nodeSupervisor = system.actorOf(Props(new NodeSupervisorActor()), "node-supervisor")

  val clientsConfigContent =
    """
      |kafka {
      |  bootstrap.servers = "localhost:9092"
      |  acks = "-1"
      |  key.serializer = "org.apache.kafka.common.serialization.StringSerializer"
      |  value.serializer = "org.apache.kafka.common.serialization.StringSerializer"
      |}
      |mqtt {
      |  uri = "tcp://localhost:1883"
      |  qos = 0
      |  retain = false
      |}
    """.stripMargin

  val khermesConfigContent =
    """
      |khermes {
      |  templates-path = "/tmp/khermes/templates"
      |  topic = "chustas"
      |  template-name = "chustasTemplate"
      |  i18n = "ES"
      |}
    """.stripMargin

  val templateContent =
    """
      |@import com.stratio.khermes.helpers.faker.Faker
      |
      |@(faker: Faker)
      |{
      |  "name" : "@(faker.Name.firstName)"
      |}
    """.stripMargin

  val avroContent =
    """
      |{
      |  "type": "record",
      |  "name": "myrecord",
      |  "fields":
      |    [
      |      {"name": "name", "type":"int"}
      |    ]
      |}
    """.stripMargin

  "An WorkerSupervisorActor" should {
    "Start n threads of working kafka producers" in {
      within(10 seconds) {
        nodeSupervisor ! Start(Seq.empty, AppConfig(khermesConfigContent, clientsConfigContent, templateContent))
        expectMsg(WorkerStatus.Started)
      }
    }
  }
}
