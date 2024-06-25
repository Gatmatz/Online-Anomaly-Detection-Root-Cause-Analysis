package transformers

import aggregators.OffsetBaselineAggregator
import aggregators.metric_aggregators.SumAggregator
import anomaly_detection.detectors.EWAppxPercentileOutlierClassifierSpec
import config.AppConfig
import models.{AggregatedRecords, AggregatedRecordsWBaseline, InputRecord}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.scalatest.flatspec.AnyFlatSpec
import sources.kafka.InputRecordStreamBuilder

class EWFeatureTransformTest extends AnyFlatSpec
{
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  AppConfig.enableCheckpoints(env)

  "test EWFeatureTransform" should "score AggregatedRecords" in {
    val spec: EWAppxPercentileOutlierClassifierSpec = new EWAppxPercentileOutlierClassifierSpec()
    spec.warmupCount = 100
    spec.sampleSize = 1000
    spec.decayPeriodType = "TUPLE_BASED"
    spec.decayPeriod = 50
    spec.decayRate = 0.01
    spec.trainingPeriodType = "TUPLE_BASED"
    spec.trainingPeriod = 50

    val inputStream: DataStream[InputRecord] = InputRecordStreamBuilder.buildInputRecordStream(env)

    val aggregatedRecordsStream: DataStream[AggregatedRecords] = inputStream
      .assignAscendingTimestamps(_.epoch)
      .windowAll(SlidingEventTimeWindows.of(Time.seconds(spec.aggregationWindowSize), Time.seconds(spec.aggregationWindowSlide)))
      .aggregate(new SumAggregator)

    // Baseline generation
    val aggregatedRecordsWBaselineStream: DataStream[AggregatedRecordsWBaseline] = aggregatedRecordsStream
      .countWindowAll(spec.elementsInBaselineOffsetWindow, 1)
      .aggregate(new OffsetBaselineAggregator)

    val featureTransform = new EWFeatureTransform(spec)

    val inputRecordsWithNorm: DataStream[(AggregatedRecordsWBaseline, Double)] = aggregatedRecordsWBaselineStream
      .keyBy(_ => 0)
      .process(featureTransform)

    inputRecordsWithNorm.print()

    env.execute("EWFeatureTransform test")
  }
}
