package anomaly_detection.detectors

import aggregators.EWFeatureTransform
import config.AppConfig
import models.{AggregatedRecordsWBaseline, InputRecord}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.scalatest.flatspec.AnyFlatSpec
import sources.kafka.InputRecordStreamBuilder

class EWAppxPercentileAuxiliaryTest extends AnyFlatSpec
{
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  AppConfig.enableCheckpoints(env)

  "test EWAppxPercentileAuxiliary" should "create tuples of Anomaly detection" in {
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

    val featureTransform = new EWFeatureTransform(spec)

    val aggregatedRecords: DataStream[AggregatedRecordsWBaseline] = inputStream
      .flatMap(featureTransform)

    val detector = new EWAppxPercentileAuxiliary(spec)

    val anomalyEventStream: DataStream[(AggregatedRecordsWBaseline, Boolean)] = aggregatedRecords
      .flatMap(detector)

    anomalyEventStream.print()
    env.execute("EWAppxPercentileOutlierDetector test")
  }
}
