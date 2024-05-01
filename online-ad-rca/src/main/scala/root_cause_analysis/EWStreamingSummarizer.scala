package root_cause_analysis
import models.{AnomalyEvent, DimensionSummary, RCAResult}
import org.apache.flink.streaming.api.datastream.DataStream
import utils.Periodic

class EWStreamingSummarizer(spec: EWStreamingSummarizerSpec, maximumSummaryDelay: Int) {
  private var streamingSummarizer: ExponentiallyDecayingEmergingItemsets = new ExponentiallyDecayingEmergingItemsets(
    spec.inlierItemSummarySize,
    spec.outlierItemSummarySize,
    spec.minSupport,
    spec.minOIRatio,
    spec.decayRate,
    spec.attributes.size,
    spec.attributeCombinations)
  private var summaryUpdater: Periodic = new Periodic(spec.decayType, spec.summaryUpdatePeriod, () => streamingSummarizer.markPeriod())
  private var summarizationTimer: Periodic = new Periodic(spec.decayType, maximumSummaryDelay, () => needsSummarization = true)
  private var count: Int = 0
  private var needsSummarization: Boolean = false

//  def runSearch(anomalyStream: DataStream[(AnomalyEvent, Boolean)]): DataStream[RCAResult] = {
//    consume(anomalyStream)
//    var summaries: List[DimensionSummary] = streamingSummarizer.getItemsets()
//    var isr: DataStream[RCAResult]= new RCAResult()
//  }

  private def consume(anomalyStream: DataStream[(AnomalyEvent, Boolean)]): Unit = {
    var passedStream: DataStream[Unit] = anomalyStream.map { eventWithFlag =>
      count = count + 1
      summaryUpdater.runIfNecessary()
      summarizationTimer.runIfNecessary()

      val (event: AnomalyEvent, isOutlier) = eventWithFlag
      if (isOutlier) {
        streamingSummarizer.markOutlier(event.aggregatedRecordsWBaseline)
      } else {
        streamingSummarizer.markInlier(event.aggregatedRecordsWBaseline)
      }
    }
  }
}