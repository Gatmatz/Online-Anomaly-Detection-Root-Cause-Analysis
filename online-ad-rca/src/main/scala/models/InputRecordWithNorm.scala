package models

import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.util.UUID
import utils.Types.{ChildDimension, DimensionName, ParentDimension}

case class InputRecordWithNorm(
                                id: String,
                                timestamp: String,
                                value: Double,
                                dimensions: Map[DimensionName, Dimension],
                                dimensions_hierarchy: Map[ChildDimension, ParentDimension],
                                norm: Double,
                                timestampPattern: String = "yyyy-MM-dd'T'HH:mm:ss"
                              ) extends Serializable {

  val parsed_timestamp: LocalDateTime =
    LocalDateTime.parse(timestamp, DateTimeFormatter.ofPattern(timestampPattern).withZone(ZoneOffset.UTC))

  val epoch: Long = parsed_timestamp.toEpochSecond(ZoneOffset.UTC) * 1000

  override def toString = {
    "InputRecordWithNorm(id=%s, created_at=%s, value=%s, dimensions=%s, dimensions_hierarchy=%s, norm=%s)".format(id, timestamp, value, dimensions, dimensions_hierarchy, norm)
  }
}

object InputRecordWithNorm {
  def apply(
             timestamp: String,
             value: Double,
             dimensions: Map[DimensionName, Dimension],
             dimensions_hierarchy: Map[ChildDimension, ParentDimension],
             norm: Double
           ): InputRecordWithNorm = {
    InputRecordWithNorm(
      id = UUID.randomUUID().toString,
      timestamp = timestamp,
      value = value,
      dimensions = dimensions,
      dimensions_hierarchy = dimensions_hierarchy,
      norm = norm
    )
  }
}
