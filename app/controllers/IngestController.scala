package org.sarlacc.controllers

import java.io._
import java.util.concurrent.atomic.AtomicLong
import java.time._
import play.api.mvc._
import play.api.libs.json._
import play.api.Logger._
import scala.util.{Try, Failure}

import org.sarlacc.services._

object Importer extends Controller {

  DataProcessor.init()
  var n = new AtomicLong(0L)

  //This is pretty ugly, but I'm doing it this way to avoid allocating any more
  //memory than necessary during parsing. A more straightforward approach would simply use
  // json formatters to transform the json directly into case classes (which is what
  // I'm trying to avoid here)
  def ingestData() = Action { request: Request[AnyContent] =>
    for {
      body <- request.body.asJson
      ls <- body.asOpt[JsArray]
      values = ls.value
      value <- values
      ts <- (value \ "timestamp").asOpt[Long].map{x => 
          //assuming local zone given. Alternatively UTC would work
          val inst = Instant.ofEpochMilli(x)
          inst.atZone(ZoneId.systemDefault).toLocalDateTime
        }
      id <- (value \ "itemId").asOpt[Int]
    }{
      Try {
        DataProcessor.submit(ts, id)
      } match {
        case Failure(e) => 
          val sw = new StringWriter
          e.printStackTrace(new PrintWriter(sw))
          println(sw.toString)
        case _ =>
          val x = n.incrementAndGet
          if(x % 10000 == 0)
            info(s"${LocalDateTime.now().toString} -> $x")
      }
    }
   
    Ok("Sarlacc consumed the data.")
  }

}
