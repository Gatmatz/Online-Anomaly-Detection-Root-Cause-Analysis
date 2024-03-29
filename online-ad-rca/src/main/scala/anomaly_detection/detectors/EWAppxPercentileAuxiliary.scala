package anomaly_detection.detectors

import models.{AggregatedRecordsWBaseline, AnomalyEvent}
import org.apache.flink.api.common.functions.{Function, MapFunction, RichFlatMapFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import utils.Periodic
import utils.sample.AdaptableDampedReservoir

import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
 * Auxiliary class for the exponentially weighted approximate percentile-based streaming classifier from Macrobase.
 * This class is a FlatMapFunction that performs the AD detection by keeping state of an ADR reservoir(sampling).
 * The function accepts a Stream of AggregatedRecordsWBaseline and emits tuples of (AggregatedRecordsWBaseline, Boolean)
 * where Boolean will be an indicator of whether the Record is an Anomaly or not.
 * @param spec the specification of the AD detection
 */
class EWAppxPercentileAuxiliary(spec: EWAppxPercentileOutlierClassifierSpec)
  extends RichFlatMapFunction[AggregatedRecordsWBaseline, (AggregatedRecordsWBaseline, Boolean)] {

  private var reservoir: AdaptableDampedReservoir[AggregatedRecordsWBaseline] = _
  private var currentThreshold: Double = 0.0
  private var reservoirDecayer: Periodic = _
  private var percentileUpdater: Periodic = _
  private var tupleCount: Int = 0
  private var warmupInput: ListBuffer[AggregatedRecordsWBaseline] = _
  override def open(parameters: Configuration): Unit = {
    reservoir = new AdaptableDampedReservoir[AggregatedRecordsWBaseline](spec.sampleSize, spec.decayRate, new Random())
    percentileUpdater = new Periodic(spec.trainingPeriodType, spec.trainingPeriod, () => updateThreshold(spec.percentile))
    reservoirDecayer = new Periodic(spec.decayPeriodType, spec.decayPeriod, () => reservoir.advancePeriod())
    warmupInput = new ListBuffer[AggregatedRecordsWBaseline]
  }

  private def updateThreshold(percentile: Double): Unit = {
    val norms: List[AggregatedRecordsWBaseline] = reservoir.getReservoir
    val sortedNorms = norms.sortBy(_.baseline)
    val index = (percentile * norms.size).toInt
    currentThreshold = sortedNorms(index).baseline
  }

  override def flatMap(value: AggregatedRecordsWBaseline, out: Collector[(AggregatedRecordsWBaseline, Boolean)]): Unit = {
    tupleCount += 1

    if (tupleCount < spec.warmupCount)
    {
      warmupInput += value
      reservoir.insert(value)
      reservoirDecayer.runIfNecessary()
      percentileUpdater.runIfNecessary()
    }
    else
    {
      if (tupleCount == spec.warmupCount)
      {
        updateThreshold(spec.percentile)
        for (record <- warmupInput)
        {
          val isAnomaly: Boolean = record.baseline > currentThreshold
          out.collect((record,isAnomaly))
        }
        warmupInput.clear()
      }

      reservoir.insert(value)
      val isAnomaly: Boolean = value.baseline > currentThreshold

      out.collect((value,isAnomaly))
    }
  }
}
