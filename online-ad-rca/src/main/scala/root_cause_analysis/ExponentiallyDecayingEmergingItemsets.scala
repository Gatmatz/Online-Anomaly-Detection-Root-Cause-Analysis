package root_cause_analysis

import models._
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
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
                                           combinationsEnabled: Boolean,
                                           summaryUpdatePeriod: Int,
                                           summarizationTime: Int
                                           ) extends KeyedProcessFunction[Int, AnomalyEvent, RCAResult] {

  private var numInliersState: ValueState[Double] = _ // Number of inliers observed
  private var numOutliersState: ValueState[Double] = _ // Number of outlier observed

  private var outlierCountSummaryState: ValueState[AmortizedMaintenanceCounter] = _ // AMC sketch for outlier attributes
  private var inlierCountSummaryState: ValueState[AmortizedMaintenanceCounter] = _ // AMC sketch for inlier attributes

  private var outlierPatternSummaryState: ValueState[StreamingFPGrowth] = _ // FPGrowth for producing explanations on the outliers
  private var inlierPatternSummaryState: ValueState[StreamingFPGrowth] = _ // FPGrowth for producing explanations on the inliers

  private var encoderState: ValueState[IntegerEncoder] = _ // Integer Encoder for translating the attributes to unique integers.

  private var interestingItems: mutable.HashMap[Int, Double] = _

  private var tupleCount: Int = _

  private var numInliers: Double = _
  private var numOutliers: Double = _
  private var outlierCountSummary: AmortizedMaintenanceCounter = _
  private var inlierCountSummary: AmortizedMaintenanceCounter = _
  private var outlierPatternSummary: StreamingFPGrowth = _
  private var inlierPatternSummary: StreamingFPGrowth = _
  private var encoder: IntegerEncoder = _

  override def open(parameters: Configuration): Unit = {
    // Keep the number of inlier state
    val numInliersDescriptor = new ValueStateDescriptor[Double](
      "numInliersState",
      classOf[Double]
    )
    numInliersState = getRuntimeContext.getState(numInliersDescriptor)

    // Keep the number of outlier state
    val numOutliersDescriptor = new ValueStateDescriptor[Double](
      "numOutliersState",
      classOf[Double]
    )
    numOutliersState = getRuntimeContext.getState(numOutliersDescriptor)

    // Keep the outlier count summary state
    val outlierCountSummaryStateDescriptor = new ValueStateDescriptor[AmortizedMaintenanceCounter](
      "outlierCountSummaryState",
      classOf[AmortizedMaintenanceCounter]
    )
    outlierCountSummaryState = getRuntimeContext.getState(outlierCountSummaryStateDescriptor)

    // Keep the outlier pattern summary state
    val inlierCountSummaryStateDescriptor = new ValueStateDescriptor[AmortizedMaintenanceCounter](
      "inlierCountSummaryState",
      classOf[AmortizedMaintenanceCounter]
    )
    inlierCountSummaryState = getRuntimeContext.getState(inlierCountSummaryStateDescriptor)


    // Keep the outlier pattern summary state
    val outlierPatternSummaryStateDescriptor = new ValueStateDescriptor[StreamingFPGrowth](
      "outlierPatternSummaryState",
      classOf[StreamingFPGrowth]
    )
    outlierPatternSummaryState = getRuntimeContext.getState(outlierPatternSummaryStateDescriptor)

    // Keep the inlier pattern summary state
    val inlierPatternSummaryStateDescriptor = new ValueStateDescriptor[StreamingFPGrowth](
      "inlierPatternSummaryState",
      classOf[StreamingFPGrowth]
    )
    inlierPatternSummaryState = getRuntimeContext.getState(inlierPatternSummaryStateDescriptor)

    // Encoder state
    val encoderStateDescriptor = new ValueStateDescriptor[IntegerEncoder](
      "encoderState",
      classOf[IntegerEncoder]
    )
    encoderState = getRuntimeContext.getState(encoderStateDescriptor)

    tupleCount = 0
  }

  override def processElement(value: AnomalyEvent, ctx: KeyedProcessFunction[Int, AnomalyEvent, RCAResult]#Context, out: Collector[RCAResult]): Unit = {
    tupleCount = tupleCount + 1

    // Fetch inliers count state
    numInliers = numInliersState.value()
    if (numInliers == null)
    {
      numInliers = 0
    }

    // Fetch outliers count state
    numOutliers = numOutliersState.value()
    if (numOutliers == null)
    {
      numOutliers = 0
    }

    // Fetch outlier count summary
    outlierCountSummary = outlierCountSummaryState.value()
    if (outlierCountSummary == null)
      {
        outlierCountSummary = new AmortizedMaintenanceCounter(outlierSummarySize)
      }

    // Fetch inlier count summary
    inlierCountSummary = inlierCountSummaryState.value()
    if (inlierCountSummary == null)
    {
      inlierCountSummary = new AmortizedMaintenanceCounter(inlierSummarySize)
    }

    // Fetch outlier pattern summary
    outlierPatternSummary = outlierPatternSummaryState.value()
    if (outlierPatternSummary == null)
    {
      outlierPatternSummary = new StreamingFPGrowth(minSupportOutlier)
    }

    // Fetch inlier pattern summary
    inlierPatternSummary = inlierPatternSummaryState.value()
    if (inlierPatternSummary == null)
    {
      inlierPatternSummary = new StreamingFPGrowth(0)
    }

    encoder = encoderState.value()
    if (encoder == null)
      {
        encoder = new IntegerEncoder()
      }


    // Check summary update time
    if (tupleCount % summaryUpdatePeriod == 0 & tupleCount != 0)
      {
        markPeriod()
      }

    // Check summarization time
    if ((tupleCount % summarizationTime == 0) & tupleCount != 0)
      {
        getItemsets.foreach(out.collect)
      }

    if (value.isOutlier)
      {
        markOutlier(value.aggregatedRecordsWBaseline)
      }
    else
      {
        markInlier(value.aggregatedRecordsWBaseline)
      }

    // Update number of inliers
    numInliersState.update(numInliers)

    // Update number of outliers
    numOutliersState.update(numOutliers)

    // Update outlier count summary
    outlierCountSummaryState.update(outlierCountSummary)

    // Update inlier count summary
    inlierCountSummaryState.update(inlierCountSummary)

    // Update outlier pattern summary
    outlierPatternSummaryState.update(outlierPatternSummary)

    // Update inlier pattern summary
    inlierPatternSummaryState.update(inlierPatternSummary)

    // Update encoder
    encoderState.update(encoder)
  }


  def updateModelsNoDecay(): Unit = {
    updateModels(doDecay = false)
  }

  private def updateModelsAndDecay(): Unit = {
    updateModels(doDecay = true)
  }

  private def updateModels(doDecay: Boolean): Unit = {
    if (!combinationsEnabled || attributeDimension == 1)
      {
        return
      }

    val outlierCounts: mutable.HashMap[Int, Double] = outlierCountSummary.getCounts
    val inlierCounts: mutable.HashMap[Int, Double] = inlierCountSummary.getCounts

    val supportCountRequired: Int = (outlierCountSummary.getTotalCount * minSupportOutlier).toInt

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

  private def markPeriod(): Unit = {
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

  private def markOutlier(outlierEvent: AggregatedRecordsWBaseline): Unit = {
    numOutliers = numOutliers + 1
    val attributes: List[Int] = getIntegerAttributes(outlierEvent)
    outlierCountSummary.observe(attributes)

    if (!combinationsEnabled || attributeDimension > 1) {
      outlierPatternSummary.insertTransactionFalseNegative(attributes.toSet)
    }
  }

  private def markInlier(inlierEvent: AggregatedRecordsWBaseline): Unit = {
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
