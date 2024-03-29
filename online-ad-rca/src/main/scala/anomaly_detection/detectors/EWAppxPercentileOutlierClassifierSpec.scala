package anomaly_detection.detectors

import anomaly_detection.AbstractDetectorSpec
import utils.Types

/**
 * Exponentially weighted approximate percentile-based streaming classifier from Macrobase.
 * This class contains the specification of the model's training and AD detection.
 */
class EWAppxPercentileOutlierClassifierSpec extends AbstractDetectorSpec
{
  //EWFeature Transformer Settings
  var warmupCount: Int = 0
  var sampleSize: Int = 0
  var decayPeriodType: Types.PeriodType = _
  var decayPeriod: Double = 0.0
  var decayRate: Double = 0.0
  var trainingPeriodType: Types.PeriodType = _
  var trainingPeriod: Double = 0.0

  //Anomaly Detector Settings
  var percentile: Double = 0.0
}
