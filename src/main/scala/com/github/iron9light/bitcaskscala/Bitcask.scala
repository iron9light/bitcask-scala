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

import java.io._

final case class BitcaskEntry(file: BitcaskFile, valueSize: Int, valuePos: Int, timestamp: Int)

object Bitcask {
  final private val DEFAULT_MAX_FILE_SIZE = Int.MaxValue // 2GB default
  final private val DEFAULT_WAIT_TIME = 4000 // 4 seconds wait time for shared keydir
}
class Bitcask(dirname: String,
              val readonly: Boolean = true,
              val maxFileSize: Int = Bitcask.DEFAULT_MAX_FILE_SIZE,
              val syncOnPut: Boolean = false,
              val waitTime: Int = Bitcask.DEFAULT_WAIT_TIME) {
  val dir = new File(dirname).getCanonicalFile
  if (!dir.exists) dir.mkdir()
  require(dir.isDirectory)

  var writeFile: Option[BitcaskFile] = None
  var readFiles: List[BitcaskFile] = Nil

  private val keyIndex = new BitcaskIndex

  def start = {
    // TODO: Check lock

    // TODO: Register

    // Init writeFile and readFiles
    val files = BitcaskFile.listDataFiles(dir)
    readFiles = files.sortBy(_.id).foldLeft(readFiles) {(list, f) => f.fillIndex(keyIndex); f :: list}

    if (!readonly) writeFile = Some(BitcaskFile.create(dir))

    this
  }

  def stop {
    // TODO: Unregister

    // TODO: Clean up all the reading files
    readFiles.map(_.close())

    // TODO: If we have a write file, assume we also currently own the write lock and cleanup both of those
    writeFile.map(_.close())
  }

  def get(key: Array[Byte]) = {
    keyIndex.get(key) match {
      case None => None
      case Some(entry) => entry.file.read(entry.valuePos, entry.valueSize)
    }
  }

  def put(key: Array[Byte], value: Array[Byte]) = {
    val file = getAndCheckWriteFile(key.length, value.length)

    val (offset, size, timestamp) = file.write(key, value)
    this.keyIndex.update(key, BitcaskEntry(file, size, offset, timestamp))

    if (syncOnPut) file.sync()
  }

  def delete(key: Array[Byte]) = {
    keyIndex.remove(key) match {
      case Some(_) =>
        val file = getAndCheckWriteFile(key.length, 0)
        file.delete(key)
      case None =>
    }
  }

  def sync() {
    writeFile.map(_.sync())
  }

  def listKeys = keyIndex.keys

  private def getAndCheckWriteFile(keySize: Int, valueSize: Int) = {
    val file0 = getWriteFile

    if (file0.checkNeedWrap(keySize, valueSize, maxFileSize)) {
      // Time to start a new write file. Note that we do not close the old
      // one, just transition it. The thinking is that closing/reopening
      // for read only access would flush the O/S cache for the file,
      // which may be undesirable.

      file0.sync()

      val newWriteFile = BitcaskFile.create(dir)

      readFiles = file0 :: readFiles
      writeFile = Some(newWriteFile)

      newWriteFile
    } else file0
  }

  private def getWriteFile = {
    this.writeFile.getOrElse(throw new IOException("Read only."))
  }
}
