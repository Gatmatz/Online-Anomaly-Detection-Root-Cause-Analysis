package models

import java.time.{LocalDateTime, ZoneOffset}
import java.util.UUID
// this should be modified and act as a report for each anomaly produced
case class AnomalyEvent(current: Double) {
  val anomaly_id: String = UUID.randomUUID().toString
  val created_at: LocalDateTime = LocalDateTime.now(ZoneOffset.UTC)

  override def toString = {
    "AnomalyEvent(id=%s, created_at=%s, current=%s)".format(anomaly_id, created_at.toString, current)
  }
}