package aggregators
import anomaly_detection.detectors.EWAppxPercentileOutlierClassifierSpec
import models.{AggregatedRecordsWBaseline, Dimension, InputRecord}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import utils.{Periodic, Types}
import utils.sample.AdaptableDampedReservoir
import utils.stats.MAD

import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
 * Class that trains a MAD model using ADR (sampling the stream).
 * The class is a FlatMap that accepts a Stream of InputRecords and emits a Stream of AggregatedRecordsWBaseline.
 * @param spec the specifications of MAD's trainer
 */
class EWFeatureTransform(spec: EWAppxPercentileOutlierClassifierSpec
                        ) extends RichFlatMapFunction[InputRecord, AggregatedRecordsWBaseline] {

  private var reservoir: AdaptableDampedReservoir[InputRecord] = _
  private var scorer: MAD = _
  private var warmupInput: ListBuffer[InputRecord] = _
  private var tupleCount: Int = 0
  private var retrainer: Periodic = _
  private var decayer: Periodic = _

  override def open(parameters: Configuration): Unit = {
    scorer = new MAD()
    reservoir = new AdaptableDampedReservoir[InputRecord](spec.sampleSize, spec.decayRate, new Random())
    warmupInput = ListBuffer.empty
    decayer = new Periodic(spec.decayPeriodType, spec.trainingPeriod, () => scorer.train(reservoir.getReservoir))
    retrainer = new Periodic(spec.trainingPeriodType, spec.decayPeriod, () => reservoir.advancePeriod())
  }

  override def flatMap(value: InputRecord, out: Collector[AggregatedRecordsWBaseline]): Unit = {
    tupleCount += 1

    if (tupleCount < spec.warmupCount)
    {
      warmupInput += value
      reservoir.insert(value)
      retrainer.runIfNecessary()
      decayer.runIfNecessary()
    }
    else
    {
      if (tupleCount == spec.warmupCount)
      {
        scorer.train(reservoir.getReservoir)
        for (record <- warmupInput)
        {
          out.collect(createRecord(record))
        }
        warmupInput.clear()
      }

      retrainer.runIfNecessary()
      decayer.runIfNecessary()
      reservoir.insert(value)
      out.collect(createRecord(value))
    }
  }

  private def createRecord(record: InputRecord): AggregatedRecordsWBaseline = {
    val score: Types.MetricValue = scorer.score(record)
    val current_mapper: Map[Dimension, Types.MetricValue] = record.dimensions.values.map(dim => (dim, record.value)).toMap
    val baseline_mapper: Map[Dimension, Types.MetricValue] = record.dimensions.values.map(dim => (dim, score)).toMap
    AggregatedRecordsWBaseline(record.value, score, current_mapper, baseline_mapper, record.dimensions_hierarchy, 1)
  }
  override def close(): Unit = {}
}
