package anomaly_detection.detectors

import anomaly_detection.AbstractDetectorSpec

class EWAppxPercentileOutlierClassifierSpec extends AbstractDetectorSpec
{
  // Reservoir settings
  val sampleSize : Int  = 0
  val warmupCount: Int = 0

//  val updatePeriodType: Types.updatePeriod
  val updatePeriod: Double = 0.0

  val decayRate: Double = 0.0
  val decayPeriod: Double = 0.0

  // Classifier settings
  val percentile: Double = 0.0

}
