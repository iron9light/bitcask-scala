/**
 * Copyright 2010 iron9light
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.iron9light.bitcaskscala

import java.util.{Arrays, TreeMap}
import collection.mutable.Map

class BitcaskIndex {
  val index = Map[Bytes, BitcaskEntry]()

  def update(key: Array[Byte], value: BitcaskEntry) {
    index.update(Bytes(key), value)
  }

  def get(key: Array[Byte]) = index.get(Bytes(key))

  def keys = index.keys.map(_.bytes)

  def remove(key: Array[Byte]) = index.remove(Bytes(key))
}

object Bytes {
  def apply(bytes: Array[Byte]) = new Bytes(bytes)
}
class Bytes(val bytes: Array[Byte]) {
  override def equals(other: Any) =
    {
      if (!other.isInstanceOf[Bytes]) false
      else Arrays.equals(bytes, other.asInstanceOf[Bytes].bytes);
    }

  override def hashCode = Arrays.hashCode(bytes)
}
