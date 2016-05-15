package org.sarlacc.services

import java.io._
import java.time.{LocalDateTime, Duration}
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.{ArrayBuffer => MArray, Map => MMap}
import scala.util._
import play.api.Logger

import org.sarlacc.aggregation._
import org.sarlacc.aggregation.Settings._
import org.sarlacc.models._

case class RollInterval(start: LocalDateTime, end: LocalDateTime){
  def in(ts: LocalDateTime): Boolean = {
    (start isBefore ts) && (end isAfter ts)
  }
}

object RollInterval {
  def make(start: LocalDateTime, duration: Duration) = {
    RollInterval(start, start.plus(duration).withSecond(0))    
  }

  def rollNextHour(start: LocalDateTime) = {
    val rollIn = Duration.ofMinutes(60 - start.getMinute - 1)
    make(start, rollIn)
  }
}

//The idea is borrowed from LMAX, but essentially multiple pages of
//the ring buffer should fit into cpu cache, meaning there are far fewer
//cache misses, leading to very high read/write throughput
abstract class RingBuffer[A, B](size: Int, f: () => A){
  private case class ResetToZero(n: Int) extends java.util.function.IntUnaryOperator {
    def applyAsInt(x: Int) = {
      if(x == n) 0 else x +1
    } 
  }
  private val reset = ResetToZero(size - 1)
  private val read = new AtomicInteger(0)
  private val write = new AtomicInteger(0)
  private val ctr = new AtomicInteger(0)
  private val buffer = MArray.fill(size)( f() )

  //This should be a function implemented 
  def offer(data: B): Unit = {
    //This flow could be cleaner using 
    val i = write.getAndUpdate(reset)
    buffer(i) = buffer(i)
    buffer(i) = doOffer(buffer(i), data)
    ctr.incrementAndGet()
  }

  protected def doOffer(cell: A, data: B): A

  def take(): Option[A] = {
    //The write pointer should always be ahead of the read pointer
    if(ctr.get() < 1 ) 
      None
    else {
      val i = read.getAndUpdate(reset)
      ctr.decrementAndGet()
      Some(buffer(i))
    }
  }


}

trait IntervalManager {
  var interval: RollInterval
  def shouldCycle(ts: LocalDateTime) = interval in ts
  def toNextHour() = {
    val s = interval.end
    interval = RollInterval.rollNextHour(s)
  }
}

object DataProcessor extends IntervalManager {
  type A = (LocalDateTime, Int)
  private case class MDataPoint(var ts: LocalDateTime, var id: Int)
  private lazy val buffer = new RingBuffer[MDataPoint, A](250000, () => MDataPoint(LocalDateTime.now(), -1)){
    //NOte this creates unnecessary garbage I should clean out
    def doOffer(cell: MDataPoint, data: (LocalDateTime, Int)) = {
      cell.ts = data._1
      cell.id = data._2
      cell
    }
  }

  var interval = RollInterval.rollNextHour(LocalDateTime.now())
  private var leadingEdge = ActiveAggregate(interval.start, interval.end, MMap.empty[Int, Int])
  
  private lazy val processor = new Thread{
    override def run() = {
      while(true){
        //this solution assumes consistent load
        Try {
          val e = buffer.take()
          e.foreach{
            case MDataPoint(ts, id) => 
              //Update the leading edge
              leadingEdge.data.get(id).fold(leadingEdge.data(id) = 1)(x => leadingEdge.data(id) += 1)
              //check if we should write a 15 minute block
              val block = ts.getMinute / PeriodMinutes
              if(ts.getMinute % PeriodMinutes == 0 && !leadingEdge.rolls(block)) {
                persist()
                leadingEdge.rolls(block) = true
                //check if we should roll the hour
                if( !interval.in(ts) ) {
                  persistHourly()
                  interval = RollInterval.rollNextHour(interval.end)
                  leadingEdge = ActiveAggregate(interval.start, interval.end, MMap.empty[Int, Int])
                }
              }
          }
        } match {
          case Failure(e) =>
            val sw = new StringWriter
            e.printStackTrace(new PrintWriter(sw))
            Logger.info(sw.toString)
          case _ =>
        }
      }
    } 
  }
  
  def init() = {
    Logger.info("Starting The Processor")    
    processor.start()
  }

  private def persist() = {
    val slice = Aggregator.aggregate(leadingEdge.begin.minusHours(23), 
                                     leadingEdge.begin, 
                                     leadingEdge) 
    
    val saved = SavedAggregate(slice.begin, LocalDateTime.now(), slice.data)
    SavedAggregate.write(saved, hourly = false)
  }

  private def persistHourly() = {
    val saved = SavedAggregate(leadingEdge.begin, LocalDateTime.now(), leadingEdge.data)
    SavedAggregate.write(saved, hourly = true)
  }

  //take the pair of values and writes them to a cell in the buffer
  def submit(ts: LocalDateTime, id: Int) =  {
     buffer.offer((ts, id))
  }
  
}

