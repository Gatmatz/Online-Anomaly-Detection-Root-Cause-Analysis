package anomaly_detection.aggregators

import models.accumulators.OffsetBaselineAccumulator
import models.{AggregatedRecords, AggregatedRecordsWBaseline, Dimension}
import org.apache.flink.api.common.functions.AggregateFunction
import utils.Types.MetricValue

/**
 * Creates records containing baseline and current values and dimensions
 */
class OffsetBaselineAggregator extends AggregateFunction[AggregatedRecords, OffsetBaselineAccumulator, AggregatedRecordsWBaseline]{
  override def createAccumulator(): OffsetBaselineAccumulator = OffsetBaselineAccumulator(
    0,
    Map[Dimension, MetricValue](),
    0,
    Seq[(Dimension, Double)](),
    0
  )

  override def add(value: AggregatedRecords, accumulator: OffsetBaselineAccumulator): OffsetBaselineAccumulator = {
    if (accumulator.current_dimensions_breakdown.isEmpty) {
      OffsetBaselineAccumulator(
        value.current,
        value.dimensions_breakdown,
        accumulator.baseline,
        accumulator.baseline_dimensions,
        accumulator.baseline_records
      )
    }
    else {
      OffsetBaselineAccumulator(
        accumulator.current,
        accumulator.current_dimensions_breakdown,
        accumulator.baseline + value.current,
        accumulator.baseline_dimensions ++ value.dimensions_breakdown.toSeq,
        accumulator.baseline_records + 1
      )
    }
  }

  /**
   * Perform averaging to get offset baseline values
   * @param accumulator
   */
  override def getResult(accumulator: OffsetBaselineAccumulator): AggregatedRecordsWBaseline = {
    AggregatedRecordsWBaseline(
      accumulator.current,
      accumulator.baseline / accumulator.baseline_records, // apply averaging
      accumulator.current_dimensions_breakdown,
      accumulator.baseline_dimensions.groupBy(_._1).mapValues(x => x.map(_._2).sum/x.length), // apply averaging in dimensions
      accumulator.baseline_records + 1) // current is always as single record
  }

  override def merge(a: OffsetBaselineAccumulator, b: OffsetBaselineAccumulator): OffsetBaselineAccumulator = {
    OffsetBaselineAccumulator(
      a.current + b.current,
      a.current_dimensions_breakdown,
      a.baseline + b.baseline,
      a.baseline_dimensions ++ b.baseline_dimensions,
      a.baseline_records + b.baseline_records
    )
  }
}
