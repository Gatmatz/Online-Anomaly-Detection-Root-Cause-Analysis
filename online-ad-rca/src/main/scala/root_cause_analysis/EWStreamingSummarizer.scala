package root_cause_analysis
import models.{AnomalyEvent, RCAResult}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
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

  private val summaryUpdater: Periodic = new Periodic(spec.decayType, spec.summaryUpdatePeriod, () => streamingSummarizer.markPeriod())
  private val summarizationTimer: Periodic = new Periodic(spec.decayType, maximumSummaryDelay, () => needsSummarization = true)
  private var count: Int = 0
  private var needsSummarization: Boolean = false

  def runSearch(anomalyStream: DataStream[(AnomalyEvent, Boolean)]): DataStream[RCAResult] = {
    val env: StreamExecutionEnvironment = anomalyStream.executionEnvironment
    consume(anomalyStream)
    val summaries: List[RCAResult] = streamingSummarizer.getItemsets
    val outputStream: DataStream[RCAResult] = env.fromCollection(summaries)
    outputStream
  }

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