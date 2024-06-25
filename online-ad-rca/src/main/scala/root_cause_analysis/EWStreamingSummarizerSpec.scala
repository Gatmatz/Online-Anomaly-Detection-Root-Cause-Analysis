package root_cause_analysis
import utils.Types

case class EWStreamingSummarizerSpec(
                                      summaryUpdatePeriod: Int,
                                      decayType: Types.PeriodType,
                                      decayRate: Double,
                                      outlierItemSummarySize: Int,
                                      inlierItemSummarySize: Int,
                                      minOIRatio: Double,
                                      minSupport: Double,
                                      attributes: List[String],
                                      attributeCombinations: Boolean,
                                      summaryGenerationPeriod: Int
                                    ) extends Serializable