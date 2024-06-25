package anomaly_detection.detectors

import aggregators.OffsetBaselineAggregator
import aggregators.metric_aggregators.SumAggregator
import anomaly_detection.AnomalyDetector
import models.{AggregatedRecords, AggregatedRecordsWBaseline, AnomalyEvent, InputRecord}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala.{DataStream, createTypeInformation}
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import transformers.EWFeatureTransform

/**
 * Exponentially weighted approximate percentile-based streaming classifier from Macrobase.
 * This class performs the whole anomaly detection process.
 */
class EWAppxPercentileOutlierClassifier extends AnomalyDetector[EWAppxPercentileOutlierClassifierSpec]{
  private var spec: EWAppxPercentileOutlierClassifierSpec = _
  override def init(spec: EWAppxPercentileOutlierClassifierSpec): Unit =
  {
    this.spec = spec
  }

  override def runDetection(inputStream: DataStream[InputRecord]): DataStream[AnomalyEvent] =
  {
    // Aggregation of input stream
    val aggregatedRecordsStream: DataStream[AggregatedRecords] = inputStream
      .assignAscendingTimestamps(_.epoch)
      .windowAll(SlidingEventTimeWindows.of(Time.seconds(spec.aggregationWindowSize), Time.seconds(spec.aggregationWindowSlide)))
      .aggregate(new SumAggregator)

    // Baseline generation
    val aggregatedRecordsWBaselineStream: DataStream[AggregatedRecordsWBaseline] = aggregatedRecordsStream
      .countWindowAll(spec.elementsInBaselineOffsetWindow, 1)
      .aggregate(new OffsetBaselineAggregator)

    // Initialize the MAD trainer
    val featureTransform = new EWFeatureTransform(spec)

    // Train MAD using ADR and assign scores to every InputRecord
    val aggregatedStreamWScore: DataStream[(AggregatedRecordsWBaseline, Double)] = aggregatedRecordsWBaselineStream
      .keyBy(_ => 0)
      .process(featureTransform)

    // Initialize the AD Detector
    val detector = new EWAppxPercentileAuxiliary(spec)

    // The AD detector will use ADR to create tuples of (Record, isAnomaly), where isAnomaly is a Boolean variable
    // indicating if the Record is an Anomaly or not.
    // The Anomalies are filtered and then they are translated to AnomalyEvent instances.
    val anomalyEventStream: DataStream[AnomalyEvent] = aggregatedStreamWScore
      .keyBy(_ => 0)
      .process(detector)  // Detect each AggregatedRecordWBaseline to Anomaly or Not

    anomalyEventStream
  }
}
