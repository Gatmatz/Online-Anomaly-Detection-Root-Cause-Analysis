package anomaly_detection.detectors

import aggregators.EWFeatureTransform
import anomaly_detection.AnomalyDetector
import models.{AggregatedRecordsWBaseline, AnomalyEvent, InputRecord}
import org.apache.flink.streaming.api.scala.{DataStream, createTypeInformation}

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
    // Initialize the MAD trainer
    val featureTransform = new EWFeatureTransform(spec)

    // Train MAD using ADR and assign scores to every InputRecord
    val inputStreamWithNorm: DataStream[AggregatedRecordsWBaseline] = inputStream
      .flatMap(featureTransform)

    // Initialize the AD Detector
    val detector = new EWAppxPercentileAuxiliary(spec)

    // The AD detector will use ADR to create tuples of (Record, isAnomaly), where isAnomaly is a Boolean variable
    // indicating if the Record is an Anomaly or not.
    // The Anomalies are filtered and then they are translated to AnomalyEvent instances.
    val anomalyEventStream: DataStream[AnomalyEvent] = inputStreamWithNorm
      .flatMap(detector)  // Detect each AggregatedRecordWBaseline to Anomaly or Not
      .filter(_._2)   // Filter out the Normal Points
      .map {
        // Specify types explicitly
        (tuple: (AggregatedRecordsWBaseline, Boolean)) =>
          val (record, _) = tuple
          AnomalyEvent(record)
      }

    anomalyEventStream
  }
}
