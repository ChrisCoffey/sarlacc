package org.sarlacc.models

import java.time.LocalDateTime

case class Error(message: String, stack: String)

case class DataPoint(timestamp: LocalDateTime, id: Int)

