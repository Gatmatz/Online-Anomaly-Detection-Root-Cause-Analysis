package models

import java.time.{LocalDateTime, ZoneOffset}
import java.util.UUID

case class AnomalyEvent(
                         anomalyId: String,
                         detectedAt: LocalDateTime,
                         epoch: Long,
                         aggregatedOrNormRecords: Record
                       ) {

  override def toString = {
    "AnomalyEvent(anomalyId=%s, detectedAt=%s, aggregatedRecordsWBaseline=%s)".format(anomalyId, detectedAt.toString, aggregatedOrNormRecords)
  }
}

object AnomalyEvent {
  def apply(aggregatedOrNormRecords: Record): AnomalyEvent = {
    val detectedAt: LocalDateTime = LocalDateTime.now(ZoneOffset.UTC)

    AnomalyEvent(
      anomalyId = UUID.randomUUID().toString,
      detectedAt = detectedAt,
      epoch = detectedAt.toEpochSecond(ZoneOffset.UTC),
      aggregatedOrNormRecords = aggregatedOrNormRecords
    )
  }
}