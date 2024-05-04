package root_cause_analysis

import models.{AggregatedRecordsWBaseline, DimensionSummary, ItemsetWithCount, RCAResult}
import utils.count.AmortizedMaintenanceCounter
import utils.encoder.IntegerEncoder
import utils.itemset.FPTree.StreamingFPGrowth
import utils.itemset.RiskRatio

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
class ExponentiallyDecayingEmergingItemsets(
                                           inlierSummarySize: Int, // Size of the heavy-hitters sketch on the inliers
                                           outlierSummarySize: Int, // Size of the heavy-hitters sketch on the outliers
                                           minSupportOutlier: Double, // Minimum support for the outliers
                                           minRatio: Double, // Minimum Risk Ratio
                                           exponentialDecayRate: Double, // Ratio to prune the sketch size
                                           attributeDimension: Int,
                                           combinationsEnabled: Boolean
                                           ) {

  private var numInliers: Double = 0 // Number of inliers observed
  private var numOutliers: Double = 0 // Number of outlier observed

  private val outlierCountSummary: AmortizedMaintenanceCounter = new AmortizedMaintenanceCounter(outlierSummarySize) // AMC sketch for outlier attributes
  private val inlierCountSummary: AmortizedMaintenanceCounter = new AmortizedMaintenanceCounter(inlierSummarySize) // AMC sketch for inlier attributes

  private val outlierPatternSummary: StreamingFPGrowth = new StreamingFPGrowth(minSupportOutlier) // FPGrowth for producing explanations on the outliers
  private val inlierPatternSummary: StreamingFPGrowth = new StreamingFPGrowth(0) // FPGrowth for producing explanations on the inliers

  private val encoder = new IntegerEncoder() // Integer Encoder for translating the attributes to unique integers.

  private var interestingItems: mutable.HashMap[Int, Double] = _

  def getInlierCount: Double = {
    numInliers
  }
  def getOutlierCount: Double = {
    numOutliers
  }

  def updateModelsNoDecay(): Unit = {
    updateModels(false)
  }

  private def updateModelsAndDecay(): Unit = {
    updateModels(true)
  }

  private def updateModels(doDecay: Boolean): Unit = {
    if (!combinationsEnabled || attributeDimension == 1)
      {
        return
      }

    val outlierCounts: mutable.HashMap[Int, Double] = this.outlierCountSummary.getCounts
    val inlierCounts: mutable.HashMap[Int, Double] = this.inlierCountSummary.getCounts

    val supportCountRequired: Int = (this.outlierCountSummary.getTotalCount * minSupportOutlier).toInt

    interestingItems = mutable.HashMap()

    for ((key, value) <- outlierCounts)
      {
        if (value < supportCountRequired)
          {

          }
        else
          {
            val inlierCount: Double = inlierCounts.getOrElse(key, -1.0)

            if (inlierCount != -1.0 && RiskRatio.compute(inlierCount, value, inlierCountSummary.getTotalCount, outlierCountSummary.getTotalCount).get() < minRatio)
              {

              }
            else
              {
                interestingItems.put(key, value)
              }
          }
      }

    val decayRate = if (doDecay) exponentialDecayRate else 0
    outlierPatternSummary.decayAndResetFrequentItems(interestingItems, decayRate)

    inlierPatternSummary.decayAndResetFrequentItems(interestingItems, decayRate)

  }

  def markPeriod(): Unit = {
    outlierCountSummary.multiplyAllCounts(1 - exponentialDecayRate)
    inlierCountSummary.multiplyAllCounts(1 - exponentialDecayRate)

    updateModelsAndDecay()
  }

  private def getIntegerAttributes(event: AggregatedRecordsWBaseline): List[Int] = {
    (event.current_dimensions_breakdown.keySet ++ event.baseline_dimensions_breakdown.keySet).map(dim => {
      val encoding = encoder.getIntegerEncoding(dim)
      encoding
    }).toList
  }

  def markOutlier(outlierEvent: AggregatedRecordsWBaseline): Unit = {
    numOutliers = numOutliers + 1
    val attributes: List[Int] = getIntegerAttributes(outlierEvent)
    outlierCountSummary.observe(attributes)

    if (!combinationsEnabled || attributeDimension > 1) {
      outlierPatternSummary.insertTransactionFalseNegative(attributes.toSet)
    }
  }

  def markInlier(inlierEvent: AggregatedRecordsWBaseline): Unit = {
    numInliers = numInliers + 1
    val attributes: List[Int] = getIntegerAttributes(inlierEvent)
    inlierCountSummary.observe(attributes)

    if (!combinationsEnabled || attributeDimension > 1)
      {
        inlierPatternSummary.insertTransactionFalseNegative(attributes.toSet)
      }
  }

  private def getSingleItemItemsets: ListBuffer[RCAResult] = {
    val supportCountRequired: Double = outlierCountSummary.getTotalCount * minSupportOutlier
    val ret: ListBuffer[RCAResult] = ListBuffer.empty[RCAResult]
    val inlierCounts = inlierCountSummary.getCounts
    val outlierCounts = outlierCountSummary.getCounts

    for ((key, value) <- outlierCounts)
      {
        if (value < supportCountRequired)
          {

          }
        else
          {
            val exposedInlierCount: Double = inlierCounts.getOrElse(key, -1.0)
            val ratio: Double = RiskRatio.compute(exposedInlierCount, value, inlierCountSummary.getTotalCount,outlierCountSummary.getTotalCount).getCorrectedRiskRatio()
            if (ratio > minRatio)
              {
                val outlierCountValue: Double = value
                val support = outlierCountValue / outlierCountSummary.getTotalCount
                val dimension = encoder.getAttribute(key)
                val dimensionSummary = DimensionSummary(dimension, support, support, ratio, outlierCountValue, outlierCountValue, outlierCountValue)
                ret += RCAResult(null, null, outlierCountValue, ratio, "1", List(dimensionSummary))
              }
          }
      }

    ret
  }

  def getItemsets : List[RCAResult] = {
    val singleItemsets = getSingleItemItemsets

    if (!combinationsEnabled || attributeDimension == 1)
      {
        return singleItemsets.toList
      }

    val iwc = outlierPatternSummary.getItemsets
    val sortedIwc = iwc.sortWith { (x, y) =>
      if (x.getCount != y.getCount)
        x.getCount > y.getCount
      else
        x.getItems.size > y.getItems.size
    }

    val ratioItemsToCheck: mutable.HashSet[Int] = mutable.HashSet.empty[Int]
    val ratioSetsToCheck: mutable.ListBuffer[ItemsetWithCount] = mutable.ListBuffer.empty[ItemsetWithCount]
    val ret: ListBuffer[RCAResult] = singleItemsets

    var prevSet: mutable.Set[Int] = null
    var prevCount: Double = -1.0

    for (i <- iwc) {
      if (i.getCount == prevCount)
        {
          if (prevSet != null && (i.getItems diff prevSet).isEmpty)
          {
            // continue
          }
          else
            {
              prevCount = i.getCount
              prevSet = i.getItems

              if (i.getItems.size != 1)
                {
                  ratioItemsToCheck ++= i.getItems
                  ratioSetsToCheck += i
                }
            }
        }
    }

    val matchingInlierCounts: List[ItemsetWithCount] = inlierPatternSummary.getCounts(ratioSetsToCheck.toList)


    assert(matchingInlierCounts.size == ratioSetsToCheck.size)
    for (i <- matchingInlierCounts.indices) {
      val ic: ItemsetWithCount = matchingInlierCounts(i)
      val oc = ratioSetsToCheck(i)

      val ratio: Double = RiskRatio.compute(ic.getCount,
        oc.getCount,
        inlierCountSummary.getTotalCount,
        outlierCountSummary.getTotalCount).getCorrectedRiskRatio()

      if (ratio >= minRatio)
        {
          val support = oc.getCount / outlierCountSummary.getTotalCount
          val outlierCountValue = oc.getCount
          val attributeSummaries = ListBuffer[DimensionSummary]()

          for (item <- oc.getItems) {
            val dimension = encoder.getAttribute(item)
            val dimensionSummary = DimensionSummary(dimension, support, support, ratio, outlierCountValue, outlierCountValue, outlierCountValue)
            attributeSummaries += dimensionSummary
          }

          val summary = RCAResult(null, null, outlierCountValue, ratio, "1", attributeSummaries.toList)
          ret += summary
        }
    }

    // finally sort one last time
    val sortedRet: ListBuffer[RCAResult] = ret.sortWith { (x: RCAResult, y:RCAResult) =>
      if (x.currentTotal != y.currentTotal)
        x.currentTotal > y.currentTotal
      else
        x.dimensionSummaries.size > y.dimensionSummaries.size
    }

    sortedRet.toList
  }

}
