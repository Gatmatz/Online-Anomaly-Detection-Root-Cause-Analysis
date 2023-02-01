package anomaly_detection

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import models.{AggregatedRecords, AnomalyEvent, InputRecord}

// https://docs.scala-lang.org/overviews/scala-book/abstract-classes.html
trait AnomalyDetector[T] extends AbstractDetectorSpec {

  def init(spec: T): Unit

  def runDetection(env: StreamExecutionEnvironment): Unit

  def mapRecordToAnomaly(record: InputRecord): AnomalyEvent = {
    AnomalyEvent(
      current=record.value
    )
  }
}