package com.github.iron9light.bitcaskscala

import java.nio.ByteBuffer
import util.continuations._
import java.lang.{Integer, Throwable}
import java.nio.channels.{AsynchronousFileChannel, CompletionHandler, AsynchronousByteChannel}

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
  protected def channel: AsynchronousFileChannel

  def read(dst: ByteBuffer, position: Long): Int@suspendable = shift {
    f: (Int => Unit) => {
      channel.read(dst, position, null, new CompletionHandler[java.lang.Integer, Any] {
        def completed(result: java.lang.Integer, attachment: Any) {
          f(result)
        }

        def failed(exc: Throwable, attachment: Any) {
          throw exc
        }
      })
    }
  }

  def write(src: ByteBuffer, position: Long): Int@suspendable = {
    shift {
      f: (Int => Unit) => {
        channel.write(src, position, null, new CompletionHandler[java.lang.Integer, Any] {
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