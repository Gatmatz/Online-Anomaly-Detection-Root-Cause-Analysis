package root_cause_analysis
import models.{AnomalyEvent, RCAResult}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import utils.Periodic

class EWStreamingSummarizer(spec: EWStreamingSummarizerSpec, maximumSummaryDelay: Int) extends Serializable {
  private val summarizer = new ExponentiallyDecayingEmergingItemsets(
                                                                      spec.inlierItemSummarySize,
                                                                      spec.outlierItemSummarySize,
                                                                      spec.minSupport,
                                                                      spec.minOIRatio,
                                                                      spec.decayRate,
                                                                      spec.attributes.size,
                                                                      spec.attributeCombinations)
  private val summaryUpdater: Periodic = new Periodic(spec.decayType, spec.summaryUpdatePeriod, () => summarizer.markPeriod())
  private val summarizationTimer: Periodic = new Periodic(spec.decayType, maximumSummaryDelay, () => needsSummarization = true)
  private var count: Int = 0
  private var needsSummarization: Boolean = false

  def consume(anomalyStream: DataStream[AnomalyEvent]): Unit = {
    anomalyStream.flatMap{
      event =>
        count = count + 1
        summaryUpdater.runIfNecessary()
        summarizationTimer.runIfNecessary()


        if (event.isOutlier) {
          summarizer.markOutlier(event.aggregatedRecordsWBaseline)
        } else {
          summarizer.markInlier(event.aggregatedRecordsWBaseline)
        }

        // Emit zero elements to prevent further downstream processing
        Seq.empty[Unit]
    }
  }

  def runSearch(env: StreamExecutionEnvironment): DataStream[RCAResult] = {
    val summaries: List[RCAResult] = summarizer.getItemsets
    val outputStream: DataStream[RCAResult] = env.fromCollection(summaries)
    outputStream
  }
}
