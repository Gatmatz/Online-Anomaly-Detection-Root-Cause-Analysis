package root_cause_analysis

import models.{AggregatedRecordsWBaseline, DimensionSummary, ItemsetWithCount, RCAResult}
import utils.count.AmortizedMaintenanceCounter
import utils.encoder.IntegerEncoder
import utils.itemset.FPTree.StreamingFPGrowth
import utils.itemset.RiskRatio

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
class ExponentiallyDecayingEmergingItemsets(
                                           inlierSummarySize: Int,
                                           outlierSummarySize: Int,
                                           minSupportOutlier: Double,
                                           minRatio: Double,
                                           exponentialDecayRate: Double,
                                           attributeDimension: Int,
                                           combinationsEnabled: Boolean
                                           ) {

  private var numInliers: Double = 0
  private var numOutliers: Double = 0

  private val outlierCountSummary: AmortizedMaintenanceCounter = new AmortizedMaintenanceCounter(outlierSummarySize)
  private val inlierCountSummary: AmortizedMaintenanceCounter = new AmortizedMaintenanceCounter(inlierSummarySize)

  private val outlierPatternSummary: StreamingFPGrowth = new StreamingFPGrowth(minSupportOutlier)
  private val inlierPatternSummary: StreamingFPGrowth = new StreamingFPGrowth(0)

  private val encoder = new IntegerEncoder()

  var interestingItems: mutable.HashMap[Int, Double] = _

  def getInlierCount: Double = {
    numInliers
  }
  def getOutlierCount: Double = {
    numOutliers
  }

  def updateModelsNoDecay(): Unit = {
    updateModels(false)
  }

  def updateModelsAndDecay(): Unit = {
    updateModels(true)
  }

  def updateModels(doDecay: Boolean): Unit = {
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

  private def getSingleItemItemsets: ListBuffer[DimensionSummary] = {
    val supportCountRequired: Double = outlierCountSummary.getTotalCount * minSupportOutlier
    val ret: ListBuffer[DimensionSummary] = ListBuffer.empty[DimensionSummary]
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
                ret += DimensionSummary(dimension, support, support, ratio, outlierCountValue, outlierCountValue, outlierCountValue)
              }
          }
      }

    ret
  }

  def getItemsets :List[RCAResult] = {
    val singleItemsets = getSingleItemItemsets

    if (!combinationsEnabled || attributeDimension == 1)
      {
        val list = ListBuffer[RCAResult]()
        list += RCAResult(null, null, 0, 0, null, singleItemsets.toList)
        return list.toList
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
    val ret = singleItemsets

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
    for (i <- 0 until matchingInlierCounts.size) {
      val ic: ItemsetWithCount = matchingInlierCounts(i)
      val oc = ratioSetsToCheck(i)

      val ratio: Double = RiskRatio.compute(ic.getCount,
        oc.getCount,
        inlierCountSummary.getTotalCount,
        outlierCountSummary.getTotalCount).getCorrectedRiskRatio()

      if (ratio >= minRatio)
        {
//          ret += DimensionSummary(oc.getCount / outlierCountSummary, oc.getCount, ratio, 0, 0, 0, 0)
        }
    }

    // finally sort one last time
//    val sortedRet = ret.sortWith { (x, y) =>
//      if (x.getNumRecords != y.getNumRecords)
//        x.getNumRecords > y.getNumRecords
//      else
//        x.getItems.size > y.getItems.size
//    }
//    sortedRet.toList
    ret.toList
  }

}
