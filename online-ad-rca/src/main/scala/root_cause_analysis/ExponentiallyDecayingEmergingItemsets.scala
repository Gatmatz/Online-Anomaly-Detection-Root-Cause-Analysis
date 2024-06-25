package root_cause_analysis

import config.AppConfig
import models._
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import utils.count.AmortizedMaintenanceCounter
import utils.encoder.IntegerEncoder
import utils.itemset.FPTree.StreamingFPGrowth
import utils.itemset.RiskRatio

import java.time.LocalDateTime
import java.util
import scala.collection.JavaConverters._
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

  private var interestingItems: util.HashMap[Int, Double] = _

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
    if (tupleCount % (summaryUpdatePeriod + 1) == 0 & tupleCount != 0)
      {
        markPeriod()
      }


    // Check summarization time
    if ((tupleCount % (summarizationTime + 1) == 0) & tupleCount != 0)
      {
        val itemsets = getItemsets().asScala
        itemsets.sortBy(-_.ratioToInliers).take(AppConfig.RootCauseAnalysis.SUMMARY_SIZE)
        itemsets.foreach(out.collect)
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

    val outlierCounts: util.HashMap[Int, Double] = outlierCountSummary.getCounts
    val inlierCounts: util.HashMap[Int, Double] = inlierCountSummary.getCounts

    val supportCountRequired: Int = (outlierCountSummary.getTotalCount * minSupportOutlier).toInt

    interestingItems = new util.HashMap[Int, Double]()
    outlierCounts.entrySet().forEach(outlierCount =>
      if (outlierCount.getValue < supportCountRequired)
        {

        }
      else
        {
          val inlierCount: Double = inlierCounts.get(outlierCount.getKey)

          if (inlierCount != null && RiskRatio.compute(inlierCount, outlierCount.getValue, inlierCountSummary.getTotalCount, outlierCountSummary.getTotalCount).get() < minRatio)
            {

            }

          interestingItems.put(outlierCount.getKey, outlierCount.getValue)
        }
    )
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
      outlierPatternSummary.insertTransactionFalseNegative(attributes.toSet.asJava)
    }
  }

  private def markInlier(inlierEvent: AggregatedRecordsWBaseline): Unit = {
    numInliers = numInliers + 1
    val attributes: List[Int] = getIntegerAttributes(inlierEvent)
    inlierCountSummary.observe(attributes)

    if (!combinationsEnabled || attributeDimension > 1)
      {
        inlierPatternSummary.insertTransactionFalseNegative(attributes.toSet.asJava)
      }
  }

  private def getSingleItemItemsets: util.List[RCAResult] = {
    val supportCountRequired: Double = outlierCountSummary.getTotalCount * minSupportOutlier
    val ret = new util.ArrayList[RCAResult]()
    val inlierCounts = inlierCountSummary.getCounts
    val outlierCounts = outlierCountSummary.getCounts

    outlierCounts.entrySet().forEach(outlierCount =>
      if (outlierCount.getValue < supportCountRequired)
        {

        }
      else
        {
          val ratio: Double = RiskRatio.compute(inlierCounts.get(outlierCount.getKey),
            outlierCount.getValue,
            inlierCountSummary.getTotalCount,
            outlierCountSummary.getTotalCount).getCorrectedRiskRatio()

          val dimension = encoder.getAttribute(outlierCount.getKey)
          val dimensionSummary = DimensionSummary(dimension, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)

          if (ratio > minRatio)
            {
              ret.add(RCAResult(null,
                LocalDateTime.now(),
                0,
                0,
                outlierCount.getValue / outlierCountSummary.getTotalCount,
                outlierCount.getValue,
                ratio,
                "all",
                List(dimensionSummary)))
            }
        }
    )
    ret
  }

  def getItemsets(): util.List[RCAResult] = {
    val singleItemsets: util.List[RCAResult] = getSingleItemItemsets

    if (!combinationsEnabled || attributeDimension == 1) {
      return singleItemsets
    }

    val iwc: util.List[ItemsetWithCount] = outlierPatternSummary.getItemsets

    iwc.sort((x, y) =>
      if (x.getCount != y.getCount)
        -java.lang.Double.compare(x.getCount, y.getCount)
      else
        -java.lang.Double.compare(x.getItems.size, y.getItems.size)
    )

    val ratioItemsToCheck: util.Set[Int] = new util.HashSet[Int]()
    val ratioSetsToCheck: util.List[ItemsetWithCount] = new util.ArrayList[ItemsetWithCount]()
    val ret: util.List[RCAResult] = singleItemsets

    var prevSet: util.Set[Int] = null
    var prevCount: Double = -1.0
    for (i <- iwc.asScala) {
      if (i.getCount == prevCount) {
        val prevSetScala = prevSet.asScala
        val iToScala = i.getItems.asScala

        if (prevSet != null && iToScala.diff(prevSetScala).isEmpty) {
          // Skip this iteration
        } else {
          prevCount = i.getCount
          prevSet = i.getItems
          if (i.getItems.size != 1) {
            ratioItemsToCheck.addAll(i.getItems)
            ratioSetsToCheck.add(i)
          }
        }
      } else {
        prevCount = i.getCount
        prevSet = i.getItems
        if (i.getItems.size != 1) {
          ratioItemsToCheck.addAll(i.getItems)
          ratioSetsToCheck.add(i)
        }
      }
    }

    // check the ratios of any itemsets we just marked
    val matchingInlierCounts: util.List[ItemsetWithCount] = inlierPatternSummary.getCounts(ratioSetsToCheck)

    assert(matchingInlierCounts.size == ratioSetsToCheck.size)
    for (i <- 0 until matchingInlierCounts.size) {
      val ic: ItemsetWithCount = matchingInlierCounts.get(i)
      val oc: ItemsetWithCount = ratioSetsToCheck.get(i)

      val ratio: Double = RiskRatio.compute(
        ic.getCount,
        oc.getCount,
        inlierCountSummary.getTotalCount,
        outlierCountSummary.getTotalCount
      ).getCorrectedRiskRatio()

      if (ratio >= minRatio) {
        val attributeSummaries = new ListBuffer[DimensionSummary]()

        oc.getItems.forEach {item =>
          val dimension = encoder.getAttribute(item)
          val dimensionSummary: DimensionSummary = DimensionSummary(dimension, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
          attributeSummaries += dimensionSummary
        }

        ret.add(RCAResult(null,
          LocalDateTime.now(),
          0,
          0,
          oc.getCount / outlierCountSummary.getTotalCount,
          oc.getCount,
          ratio,
          "all",
          attributeSummaries.toList
          ))
      }
    }

    // finally sort one last time
    ret.sort((x, y) =>
      if (x.numRecords != y.numRecords)
        -java.lang.Double.compare(x.numRecords, y.numRecords)
      else
        -java.lang.Double.compare(x.dimensionSummaries.size, y.dimensionSummaries.size)
    )

    ret
  }
}
