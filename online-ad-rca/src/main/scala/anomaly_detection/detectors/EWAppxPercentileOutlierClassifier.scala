package anomaly_detection.detectors

import aggregators.EWFeatureTransform
import anomaly_detection.AnomalyDetector
import models.{AnomalyEvent, InputRecord, InputRecordWithNorm}
import org.apache.flink.streaming.api.scala.{DataStream, createTypeInformation}
import utils.sample.AdaptableDampedReservoir

import scala.util.Random

class EWAppxPercentileOutlierClassifier extends AnomalyDetector[EWAppxPercentileOutlierClassifierSpec]{
  private var spec: EWAppxPercentileOutlierClassifierSpec = _
  private var reservoir: AdaptableDampedReservoir[InputRecordWithNorm] = _
  private var currentThreshold: Double = 0.0
  override def init(spec: EWAppxPercentileOutlierClassifierSpec): Unit =
  {
    this.spec = spec
    reservoir = new AdaptableDampedReservoir[InputRecordWithNorm](spec.sampleSize,spec.decayRate,new Random())
  }

  override def runDetection(inputStream: DataStream[InputRecord]): DataStream[AnomalyEvent] =
  {
    val featureTransform = new EWFeatureTransform(sampleSize = 100,
                                                  decayRate = 0.5,
                                                  decayPeriod = 10.0,
                                                  trainingPeriod = 20.0,
                                                  warmupCount = 50)

    // Apply the feature transformation to the input stream
    val inputRecordsWithNorm: DataStream[InputRecordWithNorm] = inputStream
      .flatMap(featureTransform)

    val anomalyEventStream: DataStream[AnomalyEvent] = inputRecordsWithNorm
      .filter(record => record.value > 10)
      .map(record => AnomalyEvent(record))

    anomalyEventStream
  }


  private def updateThreshold(percentile: Double): Unit = {
    val norms: List[InputRecordWithNorm] = reservoir.getReservoir
    val sortedNorms = norms.sortBy(_.norm)
    val index = (percentile * norms.size).toInt
    currentThreshold = sortedNorms(index).norm
  }

}
