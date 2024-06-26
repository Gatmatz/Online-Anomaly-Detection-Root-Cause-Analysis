package root_cause_analysis

import config.AppConfig
import models.{AggregatedRecordsWBaseline, AnomalyEvent, Dimension, DimensionSummary, RCAResult}
import org.apache.flink.streaming.api.scala.{DataStream, createTypeInformation}
import root_cause_analysis.HierarchicalContributorsCost.{computeChangeRatio, computeContribution}
import utils.Types.{ChildDimension, DimensionGroup, MetricValue, ParentDimension}

/**
 * According to thirdeye-pinot/src/main/java/org/apache/pinot/thirdeye/cube/cost/BalancedCostFunction.java
 *
 * Returns the cost that consider change difference, change changeRatio, and node size (contribution percentage of a node).
 */
class HierarchicalContributorsFinder extends ContributorsFinder {

  private final val MINIMUM_CONTRIBUTION_OF_INTEREST_PERCENTAGE = 3d

  override def runSearch(anomalyStream: DataStream[AnomalyEvent]): DataStream[RCAResult] = {
    anomalyStream
      // map stream of AnomalyEvent to stream of (DimensionGroup, AnomalyEvent) where is Anomaly event contains the
      // dimension breakdown of the DimensionGroup used as key
      .flatMap(record => keyByDimensionGroup(record))
      .keyBy(_._1)
      .map(anomaly => search(anomaly._2, anomaly._1))
  }

  def search(anomalyEvent: AnomalyEvent, dimensionGroup: String): RCAResult = {
    val aggregatedRecordsWBaseline: AggregatedRecordsWBaseline = anomalyEvent.aggregatedRecordsWBaseline
    val currentTotal = aggregatedRecordsWBaseline.current
    val baselineTotal =aggregatedRecordsWBaseline.baseline
    RCAResult(
      anomalyEvent.anomalyId,
      anomalyEvent.detectedAt,
      currentTotal,
      baselineTotal,
      0.0,
      0.0,
      0.0,
      dimensionGroup,
      computeSummaries(
        currentTotal,
        baselineTotal,
        aggregatedRecordsWBaseline.current_dimensions_breakdown,
        aggregatedRecordsWBaseline.baseline_dimensions_breakdown,
        aggregatedRecordsWBaseline.dimensions_hierarchy
      )
    )
  }

  def computeSummaries(
                    currentTotal: Double,
                    baselineTotal: Double,
                    currentDimensionsBreakdown: Map[Dimension, MetricValue],
                    baselineDimensionsBreakdown: Map[Dimension, MetricValue],
                    dimensionsHierarchy: Map[ChildDimension, ParentDimension]
                  ): List[DimensionSummary] = {

    // some Dimensions(name, value) tuples are not present in both tables - fill those with zeroes
    val dimensionSummaries = (currentDimensionsBreakdown.keySet ++ baselineDimensionsBreakdown.keySet).map(dim => {
      val currentValue: Double = currentDimensionsBreakdown.getOrElse(dim, 0)
      val baselineValue: Double = baselineDimensionsBreakdown.getOrElse(dim, 0)

      // compute stats
      val stats = new Stats(baselineValue, currentValue, baselineTotal, currentTotal)

      // Typically, users don't care about nodes with small contribution to overall changes
      // If contributionToOverallChangePercentage isn't above threshold then cost is 0
      if (Math.abs(stats.contributionToOverallChangePercentage) < MINIMUM_CONTRIBUTION_OF_INTEREST_PERCENTAGE) {
        DimensionSummary(
          dim,
          currentValue,
          baselineValue,
          0d,
          stats.valueChangePercentage,
          stats.contributionChangePercentage,
          stats.contributionToOverallChangePercentage
        )
      }
      else{
        /**
         * According to the implementation of AdditiveCubeNode which is used to represent a node in the
         * hierarchy graph for an additive metric get{Baseline|Current}Size() methods return {Baseline|Current}Value
         * Implementation of AdditiveCubeNode in the original ThirdEye project
         * thirdeye-pinot/src/main/java/org/apache/pinot/thirdeye/cube/additive/AdditiveCubeNode.java
         * Other than AdditiveCubeNode there is also the implementation of RatioCubeNode for ratio metrics
         */
        val baselineSize = baselineValue
        val currentSize = currentValue
        val baselineTotalSize = baselineTotal
        val currentTotalSize = currentTotal
        val parentCurrentValue = getParentValue(dim, dimensionsHierarchy, currentDimensionsBreakdown, currentTotal)
        val parentBaselineValue = getParentValue(dim, dimensionsHierarchy, baselineDimensionsBreakdown, baselineTotal)

        val parentRatio = computeChangeRatio(parentBaselineValue, parentCurrentValue)

        val contribution = computeContribution(baselineSize, currentSize, baselineTotalSize, currentTotalSize)

        val cost = HierarchicalContributorsCost.compute(baselineValue, currentValue, parentRatio, contribution)

        DimensionSummary(
          dim,
          currentValue,
          baselineValue,
          cost,
          stats.valueChangePercentage,
          stats.contributionChangePercentage,
          stats.contributionToOverallChangePercentage
        )
      }
    }).toList
      .filter(_.cost > 0) // filter out DimensionStats objects with cost <= 0

    // return filtered List of DimensionSummaries according to dimensionImportance
    dimensionImportance(dimensionSummaries, AppConfig.RootCauseAnalysis.SUMMARY_SIZE)
      .sortWith(_.cost > _.cost) // sort resulting list of DimensionStats by descending cost
  }

  private def getParentValue(
                 childDim: Dimension,
                 dimensionsHierarchy: Map[ChildDimension, ParentDimension],
                 dimensionsBreakdown: Map[Dimension, MetricValue],
                 valueTotal: Double): Double = {
    /**
     * We could add an object Dimension(root, {current|baseline}ValueTotal) in the dimensionsBreakdown
     * and not filter out root parent in DimensionHierarchy in order to handle here uniformly parent dimension handling.
     * This solution would also require Aggregators' getResult method to add the Dimension(root, {current|baseline}ValueTotal)
     *
     * But in the way that we handle it we save up space as we do not store pairs of Map[ChildDimension, ParentDimension]
     * when parent is root.
     *
     */


    // get MetricValue of parent
    // If childDim is of level 1 then parent is root so set value to valueTotal
    // otherwise search for value in dimensionsBreakdown which may be absent
    // in this case set value to 0
    val parentValue = if (childDim.level == 1) {
      valueTotal
    }
    else {
      dimensionsBreakdown.getOrElse(dimensionsHierarchy(childDim), 0d)
    }

    parentValue
  }

  def keyByDimensionGroup(record: AnomalyEvent): Seq[(DimensionGroup, AnomalyEvent)] = {
    // get all dimension groups present in this record
    val dimensionGroups = record.aggregatedRecordsWBaseline.current_dimensions_breakdown.keySet.map(_.group) ++ record.aggregatedRecordsWBaseline.baseline_dimensions_breakdown.keySet.map(_.group)

    // create a AggregatedRecordsWBaseline record for each dimension group
    dimensionGroups.map(group => {
      val groupCurrentDimensionsBreakdown = record.aggregatedRecordsWBaseline.current_dimensions_breakdown.filterKeys(_.group == group)
      val groupBaselineDimensionsBreakdown = record.aggregatedRecordsWBaseline.baseline_dimensions_breakdown.filterKeys(_.group == group)
      val groupDimensionsHierarchies = record.aggregatedRecordsWBaseline.dimensions_hierarchy.filterKeys(_.group == group)

      (group,
        AnomalyEvent(
          record.anomalyId,
          record.detectedAt,
          record.epoch,
          AggregatedRecordsWBaseline(
            record.aggregatedRecordsWBaseline.current,
            record.aggregatedRecordsWBaseline.baseline,
            groupCurrentDimensionsBreakdown,
            groupBaselineDimensionsBreakdown,
            groupDimensionsHierarchies,
            record.aggregatedRecordsWBaseline.records_in_baseline_offset
          ),
          record.isOutlier
        )
      )
    }).toSeq
  }

  /**
   * Given a LIst of DimensionSummary records from lowest to top dimension in an hierarchy keeps topK dimensions
   * with highest costs
   * @param dimensionSummaries
   * @param topK
   * @return
   */
  private def dimensionImportance(dimensionSummaries: List[DimensionSummary], topK: Int = 3): List[DimensionSummary] = {
    val dimensionSummariesByLevel = dimensionSummaries.groupBy(_.dimension.level)
    // get an ordered list od dimension levels from lowest level to upper level
    val dimensionLevels = dimensionSummariesByLevel.keySet.toList.sortWith(_ > _)

    dimensionLevels
      // get levels dimension summaries and keep topK of them according to cost
      .map(dimensionLevel => getTopKDimChildren(dimensionSummariesByLevel(dimensionLevel), topK))
      // merge dimensions summaries with those of parent level and keep topK
      .foldLeft(List[DimensionSummary]())((a,b) => getTopKDimChildren(a ++ b, topK))
  }

  private def getTopKDimChildren(dimensionSummaries: List[DimensionSummary], topK: Int): List[DimensionSummary] = {
    dimensionSummaries.sortWith(_.cost > _.cost).take(topK)
  }
}