package com.github.iron9light.bitcaskscala

import java.nio.ByteBuffer
import util.continuations._
import java.lang.Throwable
import java.nio.channels.{AsynchronousFileChannel, CompletionHandler}

object AsyncFileIoManager {

  private object completionHandler extends CompletionHandler[java.lang.Integer, Option[Throwable] => Unit] {
    def completed(result: java.lang.Integer, attachment: Option[Throwable] => Unit) {
      attachment(None)
    }

    def failed(exc: Throwable, attachment: Option[Throwable] => Unit) {
      attachment(Some(exc))
    }
  }

}

trait AsyncFileIoManager {
  protected def channel: AsynchronousFileChannel

  def read(dst: ByteBuffer, position: Long): Unit@suspendable = shift {
    f: (Option[Throwable] => Unit) => {
      channel.read(dst, position, f, AsyncFileIoManager.completionHandler)
    }
  }.foreach(throw _)

  def write(src: ByteBuffer, position: Long): Unit@suspendable = {
    shift {
      f: (Option[Throwable] => Unit) => {
        channel.write(src, position, f, AsyncFileIoManager.completionHandler)
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