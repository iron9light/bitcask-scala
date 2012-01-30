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

import java.io.{FilenameFilter, File}
import org.junit.{Before, Assert, Test}
import util.continuations._
import akka.util.duration._
import akka.util.Duration
import akka.dispatch.{ExecutionContext, Await, Promise}
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit, ThreadPoolExecutor, Executors}

class BitcaskTest {
  @Before
  def cleanup() {
    val dir = new File("./tmp")
    Option(dir.listFiles(new FilenameFilter {
      def accept(dir: File, name: String) = {
        name.matches(BitcaskFile.regex)
      }
    })).getOrElse(Array.empty).foreach(_.delete())
  }
  
  private[this] implicit val timeout = 1.second
  
  private[this] implicit val executionContext = new ThreadPoolExecutor(4, 4, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue[Runnable]) with ExecutionContext
  
  private def await[T](x: => T@suspendable)(implicit atMost: Duration) = {
    Await.result(AsFuture(x), atMost)
  }

  @Test
  def test2() {
    val n = 10

    var bitcask = new Bitcask("./tmp", false)

    await(bitcask.start)

    for (i <- 1 to n) {
      println("write k" + i)
      await(bitcask.put(("k" + i).getBytes, ("vv" + i).getBytes))
    }

    var keys = bitcask.listKeys.toSeq

    println(keys.size + " keys:")
    Assert.assertEquals(n, keys.size)

    val files = bitcask.readFiles.toSeq

    println(files.size + " files")

    files.foreach(println(_))

    for (i <- 1 to n / 2) {
      println("delete k" + i)
      await(bitcask.delete(("k" + i).getBytes))
    }

    keys = bitcask.listKeys.toSeq

    println(keys.size + " keys:")
    Assert.assertEquals(n - n / 2, keys.size)

    bitcask.stop

    bitcask = new Bitcask("./tmp")

    await(bitcask.start)

    keys = bitcask.listKeys.toSeq

    println(keys.size + " keys:")
    Assert.assertEquals(n - n / 2, keys.size)

    bitcask.stop
  }

  @Test
  def test1() {
    val bitcask = new Bitcask("./tmp", false)
    await(bitcask.start)
    for (i <- 1 to 10) {

      println("write k" + i)
      await(bitcask.put(("k" + i).getBytes, ("vv" + i).getBytes))
    }

    for (i <- 1 to 10) {
      println("read k" + i)
      val value = await(bitcask.get(("k" + i).getBytes)).get
      val expected = "vv" + i
      val actual = new String(value)
      Assert.assertArrayEquals("%s != %s".format(expected, actual), expected.getBytes, value)
    }

    bitcask.stop

    val newBitcask = new Bitcask("""./tmp""")
    Await.result(AsFuture(newBitcask.start), timeout)

    for (i <- 1 to 10) {
      val value = await(newBitcask.get(("k" + i).getBytes))
      Assert.assertArrayEquals(("vv" + i).getBytes, value.get)
    }

    newBitcask.stop
  }

  @Test
  def test3() {
    val n = 1024l * 1024
    var bitcask = new Bitcask("./tmp", false)
    val promise =
    AsFuture {
      bitcask.start
      System.gc()
      val start = System.currentTimeMillis
      var i = 0L
      while (i < n) {
        i += 1
        bitcask.put(("k" + i).getBytes, ("v" + i).getBytes)
      }
      bitcask.stop
      val time = System.currentTimeMillis - start
      println("%f s for writing %d k/vs".format(time / 1000.0, n))
      println("%f ms per writing".format(time / n.toDouble))

      bitcask = new Bitcask("./tmp")
      bitcask.start
      System.gc()
      val start2 = System.currentTimeMillis
      i = 0L
      while (i < n) {
        i += 1
        bitcask.get(("k" + i).getBytes)
      }
      bitcask.stop
      val time2 = System.currentTimeMillis - start
      println()
      println("%f s for reading %d k/vs".format(time2 / 1000.0, n))
      println("%f ms per reading".format(time2 / n.toDouble))
    }
    Await.result(promise, 10 minutes)
  }

  @Test
  def testLoadLibrary() {
    val property = System.getProperty("java.library.path")
    println(property)
    System.loadLibrary("HornetQAIO")
  }
}
