package com.github.iron9light.bitcaskscala

import util.continuations._
import akka.dispatch.{ExecutionContext, Promise}

object AsFuture {
  def apply[T](ctx: => T@suspendable)(implicit executor: ExecutionContext) = {
    val ctxR = reify[T,Unit,Unit](ctx)
    if (ctxR.isTrivial)
      Promise.successful(ctxR.getTrivialValue.asInstanceOf[T])
    else {
      val promise = Promise[T]()
      ctxR.foreachFull(promise.success(_), promise.failure(_))
      promise
    }
  }
}