package aggregators

import anomaly_detection.detectors.EWAppxPercentileOutlierClassifierSpec
import config.AppConfig
import models.{InputRecord, InputRecordWithNorm}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.scalatest.flatspec.AnyFlatSpec
import sources.kafka.InputRecordStreamBuilder
import utils.Types

class EWFeatureTransformTest extends AnyFlatSpec
{
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  AppConfig.enableCheckpoints(env)

  "test EWFeatureTransform" should "score InputRecords" in {
    val spec: EWAppxPercentileOutlierClassifierSpec = new EWAppxPercentileOutlierClassifierSpec()
    spec.warmupCount = 100
    spec.sampleSize = 1000
    spec.decayPeriodType = "TUPLE_BASED"
    spec.decayPeriod = 10
    spec.decayRate = 0.01
    spec.trainingPeriodType = "TUPLE_BASED"
    spec.trainingPeriod = 10

    val inputStream: DataStream[InputRecord] = InputRecordStreamBuilder.buildInputRecordStream(env)

    val featureTransform = new EWFeatureTransform(spec)

    val inputRecordsWithNorm: DataStream[InputRecordWithNorm] = inputStream
      .flatMap(featureTransform)

    inputRecordsWithNorm.print()

    env.execute("EWFeatureTransform test")
  }
}
