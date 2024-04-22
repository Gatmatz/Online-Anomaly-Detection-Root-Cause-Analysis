package root_cause_analysis
import utils.Types

case class EWStreamingSummarizerSpec(
                                        summaryUpdatePeriod: Double,
                                        decayType: Types.PeriodType,
                                        decayRate: Double,
                                        outlierItemSummarySize: Int,
                                        inlierItemSummarySize: Int,
                                        minOIRatio: Double,
                                        minSupport: Double,
                                        attributes: List[String],
                                        attributeCombinations: Boolean
                                      ) {
  def this(
            summaryUpdatePeriod: Double,
            decayType: Types.PeriodType,
            decayRate: Double,
            outlierItemSummarySize: Int,
            inlierItemSummarySize: Int,
            minOIRatio: Double,
            minSupport: Double,
            attributes: List[String],
            attributeCombinations: Boolean,
            maximumSummaryDelay: Int
          ) = this(summaryUpdatePeriod, decayType, decayRate, outlierItemSummarySize, inlierItemSummarySize, minOIRatio, minSupport, attributes, attributeCombinations)
}
