package models

import java.time.{LocalDateTime, ZoneOffset}
import java.util.UUID

case class AnomalyEvent(
                         anomalyId: String,
                         detectedAt: LocalDateTime,
                         epoch: Long,
                         aggregatedRecordsWBaseline: AggregatedRecordsWBaseline,
                         isOutlier: Boolean
                       ) extends Serializable {

  override def toString: String = {
    "AnomalyEvent(anomalyId=%s, detectedAt=%s, aggregatedRecordsWBaseline=%s, isOutlier=%s)".format(anomalyId, detectedAt.toString, aggregatedRecordsWBaseline, isOutlier)
  }
}

object AnomalyEvent {
  def apply(aggregatedRecordsWBaseline: AggregatedRecordsWBaseline, isOutlier:Boolean): AnomalyEvent = {
    val detectedAt: LocalDateTime = LocalDateTime.now(ZoneOffset.UTC)

    AnomalyEvent(
      anomalyId = UUID.randomUUID().toString,
      detectedAt = detectedAt,
      epoch = detectedAt.toEpochSecond(ZoneOffset.UTC),
      aggregatedRecordsWBaseline = aggregatedRecordsWBaseline,
      isOutlier = isOutlier
    )
  }
}