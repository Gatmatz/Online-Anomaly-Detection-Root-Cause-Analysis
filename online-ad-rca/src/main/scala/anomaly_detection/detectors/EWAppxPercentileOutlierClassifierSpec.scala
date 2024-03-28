package anomaly_detection.detectors

import anomaly_detection.AbstractDetectorSpec
import utils.Types

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
}
