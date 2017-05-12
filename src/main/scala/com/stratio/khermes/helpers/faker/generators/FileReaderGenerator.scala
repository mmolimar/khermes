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

package com.stratio.khermes.helpers.faker.generators

import com.stratio.khermes.commons.implicits.AppSerializer
import com.stratio.khermes.helpers.faker.FakerGenerator
import com.typesafe.scalalogging.LazyLogging

import scala.annotation.tailrec
import scala.collection.Iterator
import scala.io.Source

case class FileReaderGenerator(path: String) extends FakerGenerator
  with AppSerializer
  with LazyLogging {

  var it: Iterator[String] = init()

  override def name: String = "filereader"

  private def init(): Iterator[String] = {
    try {
      Source.fromFile(path).getLines()
    } catch {
      case ex: Throwable => logger.error(s"Cannot read file [$path]", ex)
        throw ex
    }
  }

  @tailrec
  final def readLine(): String = {
    if (it.hasNext) {
      it.next()
    } else {
      init()
      readLine()
    }
  }

}

