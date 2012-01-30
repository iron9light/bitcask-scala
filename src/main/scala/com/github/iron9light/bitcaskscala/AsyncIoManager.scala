package com.github.iron9light.bitcaskscala

import java.nio.ByteBuffer
import util.continuations._
import java.lang.{Integer, Throwable}
import java.nio.channels.{AsynchronousFileChannel, CompletionHandler}
import org.hornetq.core.asyncio.{AIOCallback, AsynchronousFile}
import java.io.IOException

trait AsyncFileIoManager {
  def read(dst: ByteBuffer, position: Long): Unit@suspendable

  def write(src: ByteBuffer, position: Long): Unit@suspendable

  def size: Long

  def sync()

  def close()
}

object AsyncFileChannelManager {

  private object completionHandler extends CompletionHandler[java.lang.Integer, Option[Throwable] => Unit] {
    def completed(result: java.lang.Integer, attachment: Option[Throwable] => Unit) {
      attachment(None)
    }

    def failed(exc: Throwable, attachment: Option[Throwable] => Unit) {
      attachment(Some(exc))
    }
  }

}

trait AsyncFileChannelManager extends AsyncFileIoManager {
  protected def channel: AsynchronousFileChannel

  def read(dst: ByteBuffer, position: Long): Unit@suspendable = shift {
    f: (Option[Throwable] => Unit) => {
      channel.read(dst, position, f, AsyncFileChannelManager.completionHandler)
    }
  }.foreach(throw _)

  def write(src: ByteBuffer, position: Long): Unit@suspendable = {
    shift {
      f: (Option[Throwable] => Unit) => {
        channel.write(src, position, f, AsyncFileChannelManager.completionHandler)
      }
    }.foreach(throw _)
  }

  def size = channel.size

  def close() {
    channel.close()
  }

  def sync() {
    channel.force(true)
  }
}

trait AsynchronousFileManager extends AsyncFileIoManager {
  protected def file: AsynchronousFile

  def read(dst: ByteBuffer, position: Long): Unit@suspendable = shift {
    f: (Option[Throwable] => Unit) =>
      file.read(position, dst.remaining, dst, new AIOCallback {
        def done() {
          f(None)
        }

        def onError(errorCode: Int, errorMessage: String) {
          f(Some(new IOException("read error[%d]%s".format(errorCode, errorMessage))))
        }
      })
  }.foreach(throw _)

  def write(src: ByteBuffer, position: Long): Unit@suspendable = shift {
    f: (Option[Throwable] => Unit) =>
      file.write(position, src.remaining, src, new AIOCallback {
        def done() {
          f(None)
        }

        def onError(errorCode: Int, errorMessage: String) {
          f(Some(new IOException("write error[%d]%s".format(errorCode, errorMessage))))
        }
      })
  }.foreach(throw _)

  def size: Long = file.size

  def sync() {
    // do nothing
  }

  def close() {
    file.close()
  }
}