package anomaly_detection.detectors

import config.AppConfig
import models.InputRecord
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.scalatest.flatspec.AnyFlatSpec
import sources.kafka.InputRecordStreamBuilder

class ThresholdDetectorTest extends AnyFlatSpec{
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  AppConfig.enableCheckpoints(env)

  "test threshold detector" should "detect anomalies" in {
    val spec: ThresholdDetectorSpec = new ThresholdDetectorSpec()

    spec.min = 3000.0f
    spec.max = 5000.0f
    spec.aggregationWindowSize = 30
    spec.elementsInBaselineOffsetWindow = 10

    val detector: ThresholdDetector = new ThresholdDetector()
    detector.init(spec)

    val inputStream: DataStream[InputRecord] = InputRecordStreamBuilder.buildInputRecordStream(env)

    detector.runDetection(inputStream)
      .print()

    env.execute("ThresholdDetector test")
  }
}