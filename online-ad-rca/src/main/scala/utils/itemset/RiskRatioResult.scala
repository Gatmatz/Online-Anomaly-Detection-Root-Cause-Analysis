package utils.itemset

/**
 * A class representing the output of a Risk Ratio computation
 * @param riskRatio risk ratio number
 * @param correction the correction of risk ratio
 */
class RiskRatioResult(private var riskRatio: Double,
                      private var correction: Double) {
  def this(riskRatio: Double) {
    this(riskRatio, 0)
  }

  def get(): Double = riskRatio

  def getCorrected(): Double = correction

  def getCorrectedRiskRatio(): Double = riskRatio - correction
}
