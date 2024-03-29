package anomaly_detection.detectors

import config.AppConfig
import models.InputRecord
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.scalatest.flatspec.AnyFlatSpec
import sources.kafka.InputRecordStreamBuilder

class EWAppxPercentileOutlierClassifierTest extends AnyFlatSpec{
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  AppConfig.enableCheckpoints(env)

  "test EWAppxPercentile detector" should "should detect Anomalies" in {
    val spec: EWAppxPercentileOutlierClassifierSpec = new EWAppxPercentileOutlierClassifierSpec()
    spec.warmupCount = 100
    spec.sampleSize = 1000
    spec.decayPeriodType = "TUPLE_BASED"
    spec.decayPeriod = 10
    spec.decayRate = 0.01
    spec.trainingPeriodType = "TUPLE_BASED"
    spec.trainingPeriod = 10
    spec.percentile = 0.9

    val inputStream: DataStream[InputRecord] = InputRecordStreamBuilder.buildInputRecordStream(env)
    val detector: EWAppxPercentileOutlierClassifier = new EWAppxPercentileOutlierClassifier()
    detector.init(spec)

    detector.runDetection(inputStream).print()
    env.execute("EWAppxPercentile test")
  }
}
