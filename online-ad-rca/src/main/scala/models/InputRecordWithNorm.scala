package models

import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.util.UUID
import utils.Types.{ChildDimension, DimensionName, ParentDimension}

case class InputRecordWithNorm(
                                record: InputRecord,
                                norm: Double
                              ) extends Record {
  override def toString = {
    "InputRecordWithNorm(id=%s, created_at=%s, value=%s, dimensions=%s, dimensions_hierarchy=%s, norm=%s)".format(record.id, record.timestamp, record.value, record.dimensions, record.dimensions_hierarchy, norm)
  }
}

object InputRecordWithNorm {
  def apply(record: InputRecord, norm: Double): InputRecordWithNorm = {
    new InputRecordWithNorm(record, norm)
  }
}