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
import java.nio.ByteBuffer
import java.nio.channels.FileChannel.MapMode

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
class BitcaskFile private(f: File, val id: Int, private val canWrite: Boolean = false) extends Closeable with CrcHelper {
  private val file = {
    if (canWrite && !f.exists) f.createNewFile()
    new RandomAccessFile(f, if (canWrite) "rw" else "r").getChannel
  }

  private var pointer = 0L
  private var needSeek = false
  private var buffer = ByteBuffer.allocateDirect(32)
  private val crc = new CRC32()

  def read(position: Int, size: Int): Option[Array[Byte]] = {
    val directBuffer = file.map(MapMode.READ_ONLY, position, size)

    val expectedCrc = directBuffer.getInt

    crc.reset()

    val timestamp = directBuffer.getInt
    updateInt(crc, timestamp)
    val keySize = directBuffer.getShort & 0xFFFF
    updateShort(crc, keySize)
    val valueSize = directBuffer.getInt
    updateInt(crc, valueSize)
    val key = new Array[Byte](keySize)
    directBuffer.get(key)
    crc.update(key)
    if (valueSize == BitcaskFile.TOMBSTONE_SIZE) {
      val actualCrc = crc.getValue.toInt
      if (expectedCrc != actualCrc) throw new IOException("CRC check failed: %s != %s".format(expectedCrc, actualCrc))
      None
    }
    else {
      val value = new Array[Byte](valueSize)
      directBuffer.get(value)
      crc.update(value)
      val actualCrc = crc.getValue.toInt
      if (expectedCrc != actualCrc) throw new IOException("CRC check failed: %s != %s".format(expectedCrc, actualCrc))
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
    if (buffer.capacity() < length) buffer = ByteBuffer.allocateDirect(length) else buffer.clear()

    crc.reset()
    buffer.position(4)
    buffer.putInt(timestamp)
    updateInt(crc, timestamp)
    buffer.putShort(keySize.toShort)
    updateShort(crc, keySize)
    buffer.putInt(valueSize)
    updateInt(crc, valueSize)
    buffer.put(key)
    crc.update(key)
    buffer.put(value)
    crc.update(value)
    
    val crcValue = crc.getValue

    buffer.putInt(0, crcValue.toInt)

    val offset = pointer
    if (needSeek) file.position(pointer)
    buffer.flip()
    do {
      file.write(buffer)
    } while (buffer.hasRemaining)

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
    if (buffer.capacity < length) buffer = ByteBuffer.allocateDirect(length) else buffer.clear()

    crc.reset()
    buffer.position(4)
    buffer.putInt(timestamp)
    updateInt(crc, timestamp)
    buffer.putShort(keySize.toShort)
    updateShort(crc, keySize)

    // valueSize = 0xFFFFFFFF
    buffer.put(BitcaskFile.TOMBSTONE)
    crc.update(BitcaskFile.TOMBSTONE)

    buffer.put(key)
    crc.update(key)

    val crcValue = crc.getValue

    buffer.putInt(0, crcValue.toInt)

    val offset = pointer
    if (needSeek) file.position(pointer)
    buffer.flip()
    do {
      file.write(buffer)
    } while (buffer.hasRemaining)

    pointer += length
    needSeek = false

    (offset.toInt, length, timestamp)
  }

  def fillIndex(index: BitcaskIndex) {
    file.position(0)

    var length = file.size
    while (length > 0) {
      val pos = file.position

      buffer.clear().limit(BitcaskFile.HEADER_SIZE)
      do {
        file.read(buffer)
      } while (buffer.hasRemaining)
      buffer.flip()
      val expectedCrc = buffer.getInt //readUInt32(buffer(0), buffer(1), buffer(2), buffer(3))

      crc.reset()

      val timestamp = buffer.getInt // readUInt32(buffer(4), buffer(5), buffer(6), buffer(7))
      updateInt(crc, timestamp)
      val keySize = buffer.getShort & 0xFFFF // readUInt16(buffer(8), buffer(9))
      updateShort(crc, keySize)
      val valueSize = buffer.getInt // readUInt32(buffer(10), buffer(11), buffer(12), buffer(13))
      updateInt(crc, valueSize)

      val key = new Array[Byte](keySize)
      file.read(ByteBuffer.wrap(key))
      crc.update(key)

      if (valueSize == BitcaskFile.TOMBSTONE_SIZE) {
        val actualCrc = crc.getValue.toInt
        if (expectedCrc != actualCrc) throw new IOException("CRC check failed: %s != %s".format(expectedCrc, actualCrc))

        index.remove(key)
      } else {
        val value = new Array[Byte](valueSize)
        file.read(ByteBuffer.wrap(value))
        crc.update(value)

        val actualCrc = crc.getValue.toInt
        if (expectedCrc != actualCrc) throw new IOException("CRC check failed: %s != %s".format(expectedCrc, actualCrc))

        index.update(key, BitcaskEntry(this, BitcaskFile.HEADER_SIZE + keySize + valueSize, pos.toInt, timestamp))
      }

      val size = file.position - pos

      length -= size
    }
  }

  def checkNeedWrap(keySize: Int, valueSize: Int, maxSize: Int): Boolean = {
    val size = BitcaskFile.HEADER_SIZE + keySize + valueSize
    (pointer + size) > maxSize
  }

  def sync() {
    file.force(false)
  }

  def close() {
    file.close()
  }
}

trait CrcHelper {
  def updateInt(crc: CRC32, i: Int) {
    crc.update(i >>> 24)
    crc.update(i >>> 16)
    crc.update(i >>> 8)
    crc.update(i)
  }
  
  def updateShort(crc: CRC32, i: Int) {
    crc.update(i >>> 8)
    crc.update(i)
  }
}