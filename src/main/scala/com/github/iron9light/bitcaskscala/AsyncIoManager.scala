package com.github.iron9light.bitcaskscala

import java.nio.ByteBuffer
import util.continuations._
import java.lang.{Integer, Throwable}
import java.nio.channels.{AsynchronousFileChannel, CompletionHandler, AsynchronousByteChannel}
import org.hornetq.core.asyncio.{AIOCallback, AsynchronousFile}
import java.io.IOException

trait AsyncIoManager {
  protected def channel: AsynchronousByteChannel
  
  def read(dst: ByteBuffer): Int@suspendable = {
    shift {
      f: (Int => Unit) => {
        channel.read(dst, null, new CompletionHandler[java.lang.Integer, Any] {
          def completed(result: java.lang.Integer, attachment: Any) {
            f(result)
          }

          def failed(exc: Throwable, attachment: Any) {
            throw exc
          }
        })
      }
    }
  }
  
  def write(src: ByteBuffer): Int@suspendable = {
    shift {
      f: (Int => Unit) => {
        channel.write(src, null, new CompletionHandler[java.lang.Integer, Any] {
          def completed(result: Integer, attachment: Any) {
            f(result)
          }

          def failed(exc: Throwable, attachment: Any) {
            throw exc
          }
        })
      }
    }
  }
}

trait AsyncFileIoManager {
  def read(dst: ByteBuffer, position: Long): Unit@suspendable

  def write(src: ByteBuffer, position: Long): Unit@suspendable

  def size: Long

  def sync()

  def close()
}

trait AsyncFileChannelManager extends AsyncFileIoManager {
  protected def channel: AsynchronousFileChannel

  def read(dst: ByteBuffer, position: Long): Unit@suspendable = shift {
    f: (Unit => Unit) => {
      channel.read(dst, position, f, new CompletionHandler[java.lang.Integer, Unit => Unit] {
        def completed(result: java.lang.Integer, attachment: Unit => Unit) {
          attachment()
        }

        def failed(exc: Throwable, attachment: Unit => Unit) {
          throw exc
        }
      })
    }
  }

  def write(src: ByteBuffer, position: Long): Unit@suspendable = {
    shift {
      f: (Unit => Unit) => {
        channel.write(src, position, f, new CompletionHandler[java.lang.Integer, Unit => Unit] {
          def completed(result: Integer, attachment: Unit => Unit) {
            attachment()
          }

          def failed(exc: Throwable, attachment: Unit => Unit) {
            throw exc
          }
        })
      }
    }
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
    f: (Unit => Unit) =>
      file.read(position, dst.remaining, dst, new AIOCallback{
        def done() {
          f()
        }
        
        def onError(errorCode: Int, errorMessage: String) {
          throw new IOException("read error[%d]%s".format(errorCode, errorMessage))
        }
      })
  }

  def write(src: ByteBuffer, position: Long): Unit@suspendable = shift {
    f: (Unit => Unit) =>
      file.write(position, src.remaining, src, new AIOCallback {
        def done() {
          f()
        }

        def onError(errorCode: Int, errorMessage: String) {
          throw new IOException("write error[%d]%s".format(errorCode, errorMessage))
        }
      })
  }

  def size: Long = file.size

  def sync() {
    // do nothing
  }

  def close() {
    file.close()
  }
}