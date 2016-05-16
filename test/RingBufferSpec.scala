package org.sarlacc.services

import scala.language.postfixOps

import org.scalacheck._
import org.scalacheck.Prop._

import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util._

object RingBufferSpec extends Properties("RingBuffer") {
  private case class BoolCell(var b: Boolean)
  private def testBuffer = new RingBuffer[BoolCell, Boolean](4,() => BoolCell(false)) {
    def doOffer(cell: BoolCell, data: Boolean) = {
      cell.b = data
      cell
    } 
  }

  property ("May read and write from same thread") = secure {
    val b = testBuffer
    val x = true
    b.offer(x)
    val Some(r) = b.take()
    r.b 
  }

  property ("May only read new writes") = secure {
    val b = testBuffer
    val x = b.take()
    val y = b.take()
    val z = b.take()
    x.isEmpty && y.isEmpty && z.isEmpty
  }

  property ("Writes block on full buffer") = secure {
    val b = testBuffer
    b.offer(true)
    b.offer(true)
    b.offer(true)
    b.offer(true)

    val f = Future { b.offer(true) }
    Try{ Await.result(f, 1 second) } match {
      case Failure(_) => true
      case _ => false
    }
  }

  property ("Reads free a blocked Write") = secure {
    val b = testBuffer
    b.offer(true)
    b.offer(true)
    b.offer(true)
    b.offer(true)

    val f = Future { b.offer(true) }
    Future { Thread.sleep(3000); b.take() }
    Try{ Await.result(f, Duration.Inf) } match {
      case Failure(_) => false
      case Success(()) => true
    }
  }

}
