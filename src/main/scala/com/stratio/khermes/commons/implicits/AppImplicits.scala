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

package com.stratio.khermes.commons.implicits

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.stratio.khermes.commons.constants.AppConstants
import com.stratio.khermes.persistence.dao.ZkDAO
import com.typesafe.config._
import com.typesafe.scalalogging.LazyLogging

/**
 * General implicits used in the application.
 */
object AppImplicits extends AppSerializer with LazyLogging {

  lazy implicit val executionContext = scala.concurrent.ExecutionContext.Implicits.global
  lazy implicit val config: Config = ConfigFactory
    .load(getClass.getClassLoader,
      ConfigResolveOptions.defaults.setAllowUnresolved(true))
    .resolve

  lazy implicit val system: ActorSystem = ActorSystem(AppConstants.AkkaClusterName, config)
  lazy implicit val configDAO: ZkDAO = new ZkDAO
  lazy implicit val materializer = ActorMaterializer()

  private type Getter[T] = (Config, String) => T

  implicit val arrayCharGetter: Getter[Array[Char]] = _ getString _ toCharArray
  implicit val arrayBytesGetter: Getter[Array[Byte]] = _ getString _ getBytes
  implicit val stringGetter: Getter[String] = _ getString _
  implicit val booleanGetter: Getter[Boolean] = _ getBoolean _
  implicit val intGetter: Getter[Int] = _ getInt _
  implicit val doubleGetter: Getter[Double] = _ getDouble _
  implicit val longGetter: Getter[Long] = _ getLong _
  implicit val configListGetter: Getter[ConfigList] = _ getList _
  implicit val configGetter: Getter[Config] = _ getConfig _
  implicit val objectGetter: Getter[ConfigObject] = _ getObject _

  implicit class ConfigOps(val config: Config) extends AnyVal {
    def getOrElse[T: Getter](path: String, defValue: => T): T = opt[T](path) getOrElse defValue

    private def opt[T: Getter](path: String): Option[T] = {
      if (config hasPathOrNull path) {
        val getter = implicitly[Getter[T]]
        Some(getter(config, path))
      } else {
        None
      }
    }
  }
}
