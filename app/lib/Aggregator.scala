package org.sarlacc.aggregation

import java.io._
import java.time._
import java.time.temporal.ChronoUnit
import scala.collection.mutable.{Map => MMap, ArrayBuffer}
import scala.io.Source
import play.api.Logger

import cats._
import cats.std.all._
import cats.implicits._

import org.sarlacc.models._

object Settings {
  lazy val cwd = new File(".").getCanonicalPath.toString
  val PeriodMinutes = 15
  val Aggregates = "aggregates"
  val Hours = "hours"
}

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
    s"${Settings.Hours}${File.separator}${t.getYear}_${t.getDayOfYear}_${t.getHour}.idx" 
  }

  def dailyFileName(t: LocalDateTime):FileName = {
    s"${Settings.Aggregates}${File.separator}${t.getYear}_${t.getDayOfYear}_${t.getHour}_${t.getMinute / Settings.PeriodMinutes }.csv"  
  }

  def write(agg: SavedAggregate, hourly: Boolean) = {
    val n = if(hourly) hourlyFileName(agg.begin) else dailyFileName(agg.end)
    val fw = new FileWriter( new File(n) )
    val pw = new PrintWriter(fw) 
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
  rolls: ArrayBuffer[Boolean] = ArrayBuffer.fill(60)(false) 
) extends TimeSlice

object Aggregator {

  private val aggs = new File(Settings.Aggregates)
  private val hours = new File(Settings.Hours)
  if(!aggs.exists)
    aggs.mkdir
  if(!hours.exists)
    hours.mkdir

  type M= MMap[Int, Int]
  private def merge(l: M, r: M): M = 
      l.foldLeft(r){
        case (acc, (key, value)) =>
          acc.get(key).fold[M]{acc(key) = value; acc }{orig => acc(key) = (orig + value); acc }
      }

  private def savedAggregates(start: LocalDateTime, end: LocalDateTime): SavedAggregate = {
    val aggregates = (0l until start.until(end, ChronoUnit.HOURS))
      .map (i => start.withMinute(0).withSecond(0).plusHours(i.toInt)) 
      .map (ts => SavedAggregate.hourlyFileName(ts) ) 
      .map {file => 
        val m = MMap.empty[Int, Int]
        Logger.info(file)
        if(new File(file).exists)
          Source.fromFile(file, "UTF-8").getLines.foreach{l =>
            val Array(id, count) = l.split(",")
            m(id.toInt) = count.toInt
          }
        m
      }
    Logger.info(s"${aggregates.length}")
    val rolledUp = aggregates.reduce(merge)
    SavedAggregate(start, end, rolledUp)
  }

  def aggregate(s: LocalDateTime, e: LocalDateTime, current: ActiveAggregate): TimeSlice = {
    val saved = savedAggregates(s, e) 
    val totalAgg = merge(saved.data, current.data)
    new TimeSlice{
      val begin = s
      val end = current.end
      val data = totalAgg
    }
  }
}
