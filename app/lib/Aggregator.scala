package org.sarlacc.aggregation

import java.io.File
import java.time._
import java.time.temporal.ChronoUnit
import scala.collection.mutable.{Map => MMap, ArrayBuffer}
import scala.io.Source

import cats._
import cats.std.all._
import cats.implicits._

import org.sarlacc.models._

trait TimeSlice {
  val begin: LocalDateTime
  val end: LocalDateTime
  val data: MMap[Int, Int]
}

case class SavedAggregate(
  begin : LocalDateTime,
  end: LocalDateTime,
  data: MMap[Int, Int]
  ) extends TimeSlice

object SavedAggregate {
  import java.io._
  type FileName = String
  def hourlyFileName(t: LocalDateTime): FileName = {
    s"hours/${t.getYear}_${t.getDayOfYear}_${t.getHour}.idx" 
  }

  def dailyFileName(t: LocalDateTime):FileName = {
    s"aggregates/${t.getYear}_${t.getDayOfYear}_${t.getHour}_${t.getMinute / 15}.csv"  
  }

  def write(agg: SavedAggregate, hourly: Boolean) = {
    val n = if(hourly) hourlyFileName(agg.begin) else dailyFileName(agg.end)
    val pw = new PrintWriter(n) 
    agg.data.foreach{
      case(key, value) => pw.println(s"$key,$value")
    }
    pw.close()
  }
}

case class ActiveAggregate( 
  begin : LocalDateTime,
  end: LocalDateTime,
  data: MMap[Int, Int],
  rolls: ArrayBuffer[Boolean] = ArrayBuffer(false, false, false, false)
) extends TimeSlice

object Aggregator {
  type M= MMap[Int, Int]
  private implicit val ev = new Monoid[M] {
    def empty = MMap.empty[Int, Int]
    def combine(l: M, r: M): M = {
      l.foldLeft(r){
        case (acc, kvp@(key, value)) =>
          acc.get(key).fold[M](acc + kvp)(orig => acc + (key->(orig + value)))
      } 
    }
  }

  private def savedAggregates(start: LocalDateTime, end: LocalDateTime): SavedAggregate = {
    val aggregates = (0l until start.until(end, ChronoUnit.HOURS))
      .map (i => start.withMinute(0).withSecond(0).plusHours(i.toInt)) 
      .map (ts => SavedAggregate.hourlyFileName(ts) ) 
      .map {file => 
        val m = MMap.empty[Int, Int]
        if(new File(file).exists)
          Source.fromFile(file, "UTF-8").getLines.foreach{l =>
            val Array(id, count) = l.split(",")
            m(id.toInt) = count.toInt
          }
        m
      }
    val rolledUp = Monoid[MMap[Int, Int]].combineAll(aggregates)
    SavedAggregate(start, end, rolledUp)
  }

  def aggregate(s: LocalDateTime, e: LocalDateTime, current: ActiveAggregate): TimeSlice = {
    val saved = savedAggregates(s, e) 
    val totalAgg = Monoid[MMap[Int, Int]].combineAll(List(saved.data, current.data))
    new TimeSlice{
      val begin = s
      val end = current.end
      val data = totalAgg
    }
  }
}
