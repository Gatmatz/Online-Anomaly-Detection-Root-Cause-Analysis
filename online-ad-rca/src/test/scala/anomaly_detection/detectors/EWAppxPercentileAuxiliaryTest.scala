package anomaly_detection.detectors

import aggregators.metric_aggregators.SumAggregator
import aggregators.OffsetBaselineAggregator
import config.AppConfig
import models.{AggregatedRecords, AggregatedRecordsWBaseline, AnomalyEvent, InputRecord}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.scalatest.flatspec.AnyFlatSpec
import sources.kafka.InputRecordStreamBuilder
import transformers.EWFeatureTransform

class EWAppxPercentileAuxiliaryTest extends AnyFlatSpec
{
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  AppConfig.enableCheckpoints(env)
  env.setParallelism(1)

  "test EWAppxPercentileAuxiliary" should "create tuples of Anomaly detection" in {
    val spec: EWAppxPercentileOutlierClassifierSpec = new EWAppxPercentileOutlierClassifierSpec()
    spec.warmupCount = 100
    spec.sampleSize = 100
    spec.decayPeriodType = "TUPLE_BASED"
    spec.decayPeriod = 50
    spec.decayRate = 0.01
    spec.trainingPeriodType = "TUPLE_BASED"
    spec.trainingPeriod = 50
    spec.percentile = 0.8

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
      .keyBy(_ => 0)
      .process(featureTransform)

    aggregatedRecordsWScore.print()

    // Anomaly Detection
    val detector = new EWAppxPercentileAuxiliary(spec)

    val anomalyEventStream: DataStream[AnomalyEvent] = aggregatedRecordsWScore
      .keyBy(_ => 0)
      .process(detector)

    anomalyEventStream.print()
    env.execute("EWAppxPercentileOutlierDetector test")
  }
}
