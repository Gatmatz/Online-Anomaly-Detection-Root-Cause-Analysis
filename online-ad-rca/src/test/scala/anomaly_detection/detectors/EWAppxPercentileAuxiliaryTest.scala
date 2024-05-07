package anomaly_detection.detectors

import aggregators.metric_aggregators.SumAggregator
import aggregators.{EWFeatureTransform, OffsetBaselineAggregator}
import config.AppConfig
import models.{AggregatedRecords, AggregatedRecordsWBaseline, AnomalyEvent, InputRecord}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
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

    //Input Stream
    val inputStream: DataStream[InputRecord] = InputRecordStreamBuilder.buildInputRecordStream(env)

    // Aggregation
    val aggregatedRecordsStream: DataStream[AggregatedRecords] = inputStream
      .assignAscendingTimestamps(_.epoch)
      .windowAll(SlidingEventTimeWindows.of(Time.seconds(spec.aggregationWindowSize), Time.seconds(spec.aggregationWindowSlide)))
      .aggregate(new SumAggregator)

    // Baseline generation
    val aggregatedRecordsWBaselineStream: DataStream[AggregatedRecordsWBaseline] = aggregatedRecordsStream
      .countWindowAll(spec.elementsInBaselineOffsetWindow, 1)
      .aggregate(new OffsetBaselineAggregator)

    // MAD training
    val featureTransform = new EWFeatureTransform(spec)

    val aggregatedRecordsWScore: DataStream[(AggregatedRecordsWBaseline, Double)] = aggregatedRecordsWBaselineStream
      .flatMap(featureTransform)

    // Anomaly Detection
    val detector = new EWAppxPercentileAuxiliary(spec)

    val anomalyEventStream: DataStream[AnomalyEvent] = aggregatedRecordsWScore
      .flatMap(detector)

    anomalyEventStream.print()
    env.execute("EWAppxPercentileOutlierDetector test")
  }
}
