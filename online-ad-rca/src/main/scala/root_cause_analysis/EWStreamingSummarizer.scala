package root_cause_analysis
import models.{AnomalyEvent, RCAResult}
import org.apache.flink.streaming.api.scala.{DataStream, createTypeInformation}

class EWStreamingSummarizer(spec: EWStreamingSummarizerSpec, maximumSummaryDelay: Int) extends SimpleContributorsFinder {
  override def runSearch(anomalyStream: DataStream[AnomalyEvent]): DataStream[RCAResult] = {
    val summarizer: ExponentiallyDecayingEmergingItemsets = new ExponentiallyDecayingEmergingItemsets(
      spec.inlierItemSummarySize,
      spec.outlierItemSummarySize,
      spec.minSupport,
      spec.minOIRatio,
      spec.decayRate,
      spec.attributes.size,
      spec.attributeCombinations,
      spec.summaryUpdatePeriod,
      maximumSummaryDelay
    )

    anomalyStream
      .keyBy(_ => 0)
      .process(summarizer)
  }
}