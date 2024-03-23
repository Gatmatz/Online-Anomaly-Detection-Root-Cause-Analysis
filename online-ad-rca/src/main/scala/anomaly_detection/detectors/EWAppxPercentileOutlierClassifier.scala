//package anomaly_detection.detectors
//
//import anomaly_detection.AnomalyDetector
//import models.{AnomalyEvent, InputRecord}
//import org.apache.flink.streaming.api.scala.DataStream
//import utils.sample.AdaptableDampedReservoir
//
//import scala.util.Random
//
//class EWAppxPercentileOutlierClassifier extends AnomalyDetector[EWAppxPercentileOutlierClassifierSpec]{
//  private var spec: EWAppxPercentileOutlierClassifierSpec = _
//  private var reservoir: AdaptableDampedReservoir[InputRecord] = _
//  private var currentThreshold: Double = 0.0
//  override def init(spec: EWAppxPercentileOutlierClassifierSpec): Unit =
//  {
//    this.spec = spec
//    reservoir = new AdaptableDampedReservoir[InputRecord](spec.sampleSize,spec.decayRate,new Random())
//  }
//
//  override def runDetection(inputStream: DataStream[InputRecord]): DataStream[AnomalyEvent] =
//  {
//   val output: DataStream[AnomalyEvent] = inputStream
//    return output
//  }
//
//  private def updateThreshold(percentile: Double): Unit = {
//    val norms: List[InputRecord] = reservoir.getReservoir
//    val sortedNorms = norms.sortBy(_.getNorm())
//    val index = (percentile * norms.size).toInt
//    currentThreshold = sortedNorms(index).getNorm()
//  }
//
//}
