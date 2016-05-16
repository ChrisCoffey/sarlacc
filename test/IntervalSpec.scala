package org.sarlacc.services

import java.time._
import scala.util._

import org.scalacheck._
import org.scalacheck.Arbitrary._
import org.scalacheck.Prop._

object IntervalSpec extends Properties("RollInterval") {
  import TimeGens.{arbLdt, arbInterval}
 
  property ("Requires End after start") = forAll {(s: LocalDateTime, e: LocalDateTime) =>
    Try {
      RollInterval(s,e)
    } match {
      case Success(_) => s isBefore e
      case Failure(_) => s isAfter e
    }
  }

  property ("Answers whether a value is in the interval") = forAll{(i: RollInterval, x: LocalDateTime) =>
    if(i.in(x)){
      (i.start isBefore x) &&  (i.end isAfter x)
    } else {
      (i.start isAfter x) ||  (i.end isBefore x)
    }
  }
}

object TimeGens {
  implicit lazy val arbLdt: Arbitrary[LocalDateTime] = Arbitrary(ldt)
  implicit lazy val arbInterval: Arbitrary[RollInterval] = Arbitrary(interval)

  val ldt: Gen[LocalDateTime] = for {
    day <- Gen.choose(1,30)
    hour <- Gen.choose(0,23)
  } yield LocalDateTime.of(2016, 5, day, hour, 0)

  val interval: Gen[RollInterval] = 
    ldt.map(s => RollInterval(s, s.plusHours(1)))
}
