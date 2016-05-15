package org.sarlacc.intg

import play.api.libs.json._
import java.time._
import scalaj.http._
import org.scalacheck.Gen


object IntgGens {
 val validId = Gen.choose(0, 10000)
 val point = 
   validId.map{id =>
    val ts = System.currentTimeMillis
    Json.obj("timestamp" -> ts, "itemId" -> id)
   }

 val message = for {
  i <- Gen.choose(1,100)
  ls <- Gen.listOfN(i, point)
 } yield {
  JsArray(ls) 
 }

}

object TestClient {
  val target = "http://localhost:9000/data"
  var n = 0

  def main(args: Array[String]): Unit = {
    while(true){
      val ls = IntgGens.message.sample.get
      Http(target).postData(ls.toString).header("content-type", "application/json").asString
      n += 1
      if(n % 1000 == 0)
        println(s"${LocalDateTime.now().toString} -> $n")
    }
  }



}
