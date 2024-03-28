package models

import java.time.{LocalDateTime, ZoneOffset}
import java.util.UUID

case class AnomalyEvent(
                         anomalyId: String,
                         detectedAt: LocalDateTime,
                         epoch: Long,
                         aggregatedRecordsWBaseline: Record
                       ) {

  override def toString: String = {
    "AnomalyEvent(anomalyId=%s, detectedAt=%s, aggregatedRecordsWBaseline=%s)".format(anomalyId, detectedAt.toString, aggregatedRecordsWBaseline)
  }
}

object AnomalyEvent {
  def apply(aggregatedRecordsWBaseline: AggregatedRecordsWBaseline): AnomalyEvent = {
    val detectedAt: LocalDateTime = LocalDateTime.now(ZoneOffset.UTC)

    AnomalyEvent(
      anomalyId = UUID.randomUUID().toString,
      detectedAt = detectedAt,
      epoch = detectedAt.toEpochSecond(ZoneOffset.UTC),
      aggregatedRecordsWBaseline = aggregatedRecordsWBaseline
    )
  }

  def apply(aggregatedRecordsWBaseline: InputRecordWithNorm): AnomalyEvent = {
    val detectedAt: LocalDateTime = LocalDateTime.now(ZoneOffset.UTC)

    AnomalyEvent(
      anomalyId = UUID.randomUUID().toString,
      detectedAt = detectedAt,
      epoch = detectedAt.toEpochSecond(ZoneOffset.UTC),
      aggregatedRecordsWBaseline = aggregatedRecordsWBaseline
    )
  }
}