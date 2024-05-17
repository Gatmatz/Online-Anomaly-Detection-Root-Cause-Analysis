package utils.itemset

/**
 * A class representing the Risk Ratio computation.
 */
object RiskRatio {
  private def computeDouble(exposedInlierCount: Double,
                            exposedOutlierCount: Double,
                            totalInliers: Double,
                            totalOutliers: Double): RiskRatioResult = {
    val totalExposedCount = exposedInlierCount + exposedOutlierCount
    val totalMinusExposedCount = totalInliers + totalOutliers - totalExposedCount
    val unexposedOutlierCount = totalOutliers - exposedOutlierCount
    val unexposedInlierCount = totalInliers - exposedInlierCount

    // No exposure found
    if (totalExposedCount == 0) {
      return new RiskRatioResult(0)
    }

    // No exposed outliers
    if (exposedOutlierCount == 0) {
      return new RiskRatioResult(0)
    }

    // we only exposed this ratio, everything matched!
    if (totalMinusExposedCount == 0) {
      return new RiskRatioResult(0)
    }

    // all outliers had this pattern
    if (unexposedOutlierCount == 0) {
      return new RiskRatioResult(Double.PositiveInfinity)
    }

    val z = 2.0
    val correction = z * Math.sqrt(
      (exposedInlierCount / exposedOutlierCount) / totalExposedCount +
        (unexposedInlierCount / unexposedInlierCount) / totalMinusExposedCount
    )

    new RiskRatioResult((exposedOutlierCount / totalExposedCount) /
      (unexposedOutlierCount / totalMinusExposedCount), correction)
  }

  def compute(exposedInlierCount: Double,
              exposedOutlierCount: Double,
              totalInliers: Double,
              totalOutliers: Double): RiskRatioResult = {
    val exposedInlierCountChecked = {
      if (exposedInlierCount == -1.0) {
        0.0
      }
      else
        exposedInlierCount
    }

    val exposedOutlierCountChecked = {
      if (exposedOutlierCount == -1.0) {
        0.0
      }
      else
        exposedOutlierCount
    }

    val totalInliersChecked = {
      if (totalInliers == -1.0) {
        0.0
      }
      else
        totalInliers
    }

    val totalOutliersChecked = {
      if (totalOutliers == -1.0) {
        0.0
      }
      else
        totalOutliers
    }


    computeDouble(exposedInlierCountChecked, exposedOutlierCountChecked, totalInliersChecked, totalOutliersChecked)
  }
}