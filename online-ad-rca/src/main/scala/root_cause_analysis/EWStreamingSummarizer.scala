package root_cause_analysis
import models.{AnomalyEvent, RCAResult}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, createTypeInformation}
import org.apache.flink.util.Collector
import utils.Types

import java.time.Duration

class EWStreamingSummarizer(spec: EWStreamingSummarizerSpec, maximumSummaryDelay: Int) extends ContributorsFinder {

  val summarizer = new ExponentiallyDecayingEmergingItemsets(
    spec.inlierItemSummarySize,
    spec.outlierItemSummarySize,
    spec.minSupport,
    spec.minOIRatio,
    spec.decayRate,
    spec.attributes.size,
    spec.attributeCombinations)

  private val summaryUpdater = new SummaryUpdater(spec.decayType, spec.summaryUpdatePeriod)
  private val summarizationTimer = new SummarizationTimer(spec.decayType, maximumSummaryDelay)
  private var needsSummarization = false

  def runSearch(anomalyStream: DataStream[AnomalyEvent]): DataStream[RCAResult] = {
    anomalyStream
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness[AnomalyEvent](Duration.ofSeconds(0))
          .withTimestampAssigner(new SerializableTimestampAssigner[AnomalyEvent] {
            private var i: Long = 0
            override def extractTimestamp(event: AnomalyEvent, recordTimestamp: Long): Long = {
              i = i + 1
              event.epoch + i
            }
          })
      )
      .keyBy(_ => 0) // Key all events by a constant key to ensure single-threaded processing
      .process(new SummarizationProcessFunction)
  }

  private class SummaryUpdater(periodType: Types.PeriodType, periodLength: Double) extends Serializable {
    private var previousPeriod: Double = _
    private var numCalls: Int = _
    private var elapsed: Double = _

    if (periodType == "TIME_BASED") {
      previousPeriod = System.currentTimeMillis()
    }

    def runIfNecessary(): Unit = {
      numCalls += 1
      if (periodLength < 0) {
        return
      }
      elapsed = {
        if (periodType == "TIME_BASED") {
          System.currentTimeMillis()
        } else {
          numCalls
        }
      }
      while (previousPeriod + periodLength < elapsed) {
        summarizer.markPeriod()
        previousPeriod += periodLength
      }
    }
  }

  private class SummarizationTimer(periodType: Types.PeriodType, periodLength: Double) extends Serializable {
    private var previousPeriod: Double = _
    private var numCalls: Int = _
    private var elapsed: Double = _

    if (periodType == "TIME_BASED") {
      previousPeriod = System.currentTimeMillis()
    }

    def runIfNecessary(): Unit = {
      numCalls += 1
      if (periodLength < 0) {
        return
      }
      elapsed = {
        if (periodType == "TIME_BASED") {
          System.currentTimeMillis()
        } else {
          numCalls
        }
      }
      while (previousPeriod + periodLength < elapsed) {
        needsSummarization = true
        previousPeriod += periodLength
      }
    }
  }

  private class SummarizationProcessFunction extends KeyedProcessFunction[Int, AnomalyEvent, RCAResult] {
    lazy val countState: ValueState[Int] = getRuntimeContext.getState(new ValueStateDescriptor[Int]("count", classOf[Int]))

    override def processElement(value: AnomalyEvent, ctx: KeyedProcessFunction[Int, AnomalyEvent, RCAResult]#Context, out: Collector[RCAResult]): Unit = {
      // Update the counter
      val count = countState.value() + 1
      countState.update(count)

      // Run the summary updater and timer
      summaryUpdater.runIfNecessary()
      summarizationTimer.runIfNecessary()

      // Update the summarizer based on the event type
      if (value.isOutlier) {
        summarizer.markOutlier(value.aggregatedRecordsWBaseline)
      } else {
        summarizer.markInlier(value.aggregatedRecordsWBaseline)
      }

      ctx.timerService().registerEventTimeTimer(ctx.timestamp())
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Int, AnomalyEvent, RCAResult]#OnTimerContext, out: Collector[RCAResult]): Unit = {
      if (needsSummarization)
        {
          // Emit all summaries when the timer fires
          summarizer.getItemsets.foreach(out.collect)
          needsSummarization = false
        }
    }
  }
}
