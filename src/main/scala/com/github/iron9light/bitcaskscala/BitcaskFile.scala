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

import java.util.zip.CRC32
import java.io._

object BitcaskFile {
  def create(dir: File): BitcaskFile = {
    val id = getTimestamp
    val file = new File(dir, id.toString + ".bitcask.data")
    new BitcaskFile(file, id, true)
  }

  def loadFile(f: File) = {
    val id = getId(f.getName)
    new BitcaskFile(f, id, false)
  }

  def listDataFiles(dir: File): Array[BitcaskFile] = {
    val files = dir.listFiles(new FilenameFilter {
      def accept(dir: File, name: String): Boolean = {
        name.matches(regex)
      }
    }).map(loadFile(_))

    files
  }

  private[bitcaskscala] final val regex = """([0-9]+)\.bitcask\.data"""
  private final val MAXKEYSIZE = Int.MaxValue
  private final val MAXVALSIZE = Long.MaxValue
  private final val HEADER_SIZE = 14 // 4 + 4 + 2 + 4 bytes

  private final val TOMBSTONE = Array(0xFF.toByte, 0xFF.toByte, 0xFF.toByte, 0xFF.toByte)
  private final val TOMBSTONE_SIZE = 0xFFFFFFFF

  private def getTimestamp = (System.currentTimeMillis / 1000).toInt

  private def getId(fileName: String) = fileName.takeWhile(c => c >= '0' && c <= '9').toInt
}
class BitcaskFile private(f: File, val id: Int, private val canWrite: Boolean = false) extends Closeable {
  private val file = {
    if (canWrite && !f.exists) f.createNewFile()
    new RandomAccessFile(f, if (canWrite) "rw" else "r")
  }

  private var pointer = 0L
  private var needSeek = false
  private var buffer = new Array[Byte](32)
  private val crc = new CRC32()

  def read(position: Int, size: Int): Option[Array[Byte]] = {
    if (buffer.length < size) buffer = new Array[Byte](size)

    file.seek(position)
    file.read(buffer, 0, size)
    if (canWrite && position + size != pointer) needSeek = true

    val expectedCrc = readUInt32(buffer(0), buffer(1), buffer(2), buffer(3))

    crc.reset()
    crc.update(buffer, 4, size - 4)
    var actualCrc = crc.getValue.toInt
    if (expectedCrc != actualCrc) throw new IOException("CRC check failed: %s != %s".format(expectedCrc, actualCrc))

    val timestamp = readUInt32(buffer(4), buffer(5), buffer(6), buffer(7))
    val keySize = readUInt16(buffer(8), buffer(9))
    val valueSize = readUInt32(buffer(10), buffer(11), buffer(12), buffer(13))
    if (valueSize == BitcaskFile.TOMBSTONE_SIZE) None
    else {
      val value = new Array[Byte](valueSize)
      Array.copy(buffer, BitcaskFile.HEADER_SIZE + keySize, value, 0, valueSize)
      Some(value)
    }
  }

  def write(key: Array[Byte], value: Array[Byte]): (Int, Int, Int) = {
    val timestamp = BitcaskFile.getTimestamp
    val keySize = key.length
    require(keySize <= BitcaskFile.MAXKEYSIZE)
    val valueSize = value.length
    require(valueSize <= BitcaskFile.MAXVALSIZE)

    val length = BitcaskFile.HEADER_SIZE + keySize + valueSize
    if (buffer.length < length) buffer = new Array[Byte](length)

    writeInt32(timestamp, buffer, 4)
    writeInt16(keySize, buffer, 8)
    writeInt32(valueSize, buffer, 10)
    key.copyToArray(buffer, 14)
    value.copyToArray(buffer, 14 + keySize)

    crc.reset()
    crc.update(buffer, 4, length - 4)
    val crcValue = crc.getValue

    writeInt32(crcValue, buffer, 0)

    val offset = pointer
    if (needSeek) file.seek(pointer)
    file.write(buffer, 0, length)

    pointer += length
    needSeek = false

    (offset.toInt, length, timestamp)
  }

  def delete(key: Array[Byte]): (Int, Int, Int) = {
    val timestamp = BitcaskFile.getTimestamp
    val keySize = key.length
    require(keySize <= BitcaskFile.MAXKEYSIZE)
    //    val valueSize = 0xFFFFFFFF

    val length = BitcaskFile.HEADER_SIZE + keySize
    if (buffer.length < length) buffer = new Array[Byte](length)

    writeInt32(timestamp, buffer, 4)
    writeInt16(keySize, buffer, 8)

    // valueSize = 0xFFFFFFFF
    BitcaskFile.TOMBSTONE.copyToArray(buffer, 10)

    key.copyToArray(buffer, 14)

    crc.reset()
    crc.update(buffer, 4, length - 4)
    val crcValue = crc.getValue

    writeInt32(crcValue, buffer, 0)

    val offset = pointer
    if (needSeek) file.seek(pointer)
    file.write(buffer, 0, length)

    pointer += length
    needSeek = false

    (offset.toInt, length, timestamp)
  }

  def fillIndex(index: BitcaskIndex) {
    file.seek(0)

    var length = file.length
    while (length > 0) {
      val pos = file.getFilePointer

      file.read(buffer, 0, BitcaskFile.HEADER_SIZE)
      val expectedCrc = readUInt32(buffer(0), buffer(1), buffer(2), buffer(3))

      crc.reset()
      crc.update(buffer, 4, BitcaskFile.HEADER_SIZE - 4)

      val timestamp = readUInt32(buffer(4), buffer(5), buffer(6), buffer(7))
      val keySize = readUInt16(buffer(8), buffer(9))
      val valueSize = readUInt32(buffer(10), buffer(11), buffer(12), buffer(13))

      val key = new Array[Byte](keySize)
      file.read(key)
      crc.update(key)

      if (valueSize == BitcaskFile.TOMBSTONE_SIZE) {
        var actualCrc = crc.getValue.toInt
        if (expectedCrc != actualCrc) throw new IOException("CRC check failed: %s != %s".format(expectedCrc, actualCrc))

        index.remove(key)
      } else {
        val value = new Array[Byte](valueSize)
        file.read(value)
        crc.update(value)

        var actualCrc = crc.getValue.toInt
        if (expectedCrc != actualCrc) throw new IOException("CRC check failed: %s != %s".format(expectedCrc, actualCrc))

        index.update(key, BitcaskEntry(this, BitcaskFile.HEADER_SIZE + keySize + valueSize, pos.toInt, timestamp))
      }

      val size = file.getFilePointer - pos

      length -= size
    }
  }

  def checkNeedWrap(keySize: Int, valueSize: Int, maxSize: Int): Boolean = {
    val size = BitcaskFile.HEADER_SIZE + keySize + valueSize
    (pointer + size) > maxSize
  }

  def sync() {
    // TODO
  }

  def close() {
    file.close()
  }

  @inline
  private def readUInt32(a: Byte, b: Byte, c: Byte, d: Byte) = {
    (a & 0xFF) << 24 | (b & 0xFF) << 16 | (c & 0xFF) << 8 | (d & 0xFF) << 0
  }

  @inline
  private def readUInt16(a: Byte, b: Byte) = (a & 0xFF) << 8 | (b & 0xFF) << 0

  @inline
  private def writeInt32(value: Int, buffer: Array[Byte], start: Int) {
    buffer.update(start, (value >>> 24).toByte)
    buffer.update(start + 1, (value >>> 16).toByte)
    buffer.update(start + 2, (value >>> 8).toByte)
    buffer.update(start + 3, value.byteValue)
  }

  @inline
  private def writeInt32(value: Long, buffer: Array[Byte], start: Int) {
    buffer.update(start, (value >>> 24).toByte)
    buffer.update(start + 1, (value >>> 16).toByte)
    buffer.update(start + 2, (value >>> 8).toByte)
    buffer.update(start + 3, value.toByte)
  }

  @inline
  private def writeInt16(value: Int, buffer: Array[Byte], start: Int) {
    buffer.update(start, (value >>> 8).toByte)
    buffer.update(start + 1, value.toByte)
  }
}