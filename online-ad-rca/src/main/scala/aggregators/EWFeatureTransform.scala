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
 * The class is a FlatMap that accepts a Stream of AggregatedRecordsWBaseline and emits a Stream of tuples in the
 * form of (AggregatedRecordsWBaseline, Score).
 * @param spec the specifications of MAD's trainer
 */
class EWFeatureTransform(spec: EWAppxPercentileOutlierClassifierSpec
                        ) extends RichFlatMapFunction[AggregatedRecordsWBaseline, (AggregatedRecordsWBaseline, Double)] {

  private var reservoir: AdaptableDampedReservoir[AggregatedRecordsWBaseline] = _
  private var scorer: MAD = _
  private var warmupInput: ListBuffer[AggregatedRecordsWBaseline] = _
  private var tupleCount: Int = 0
  private var retrainer: Periodic = _
  private var decayer: Periodic = _

  override def open(parameters: Configuration): Unit = {
    scorer = new MAD()
    reservoir = new AdaptableDampedReservoir[AggregatedRecordsWBaseline](spec.sampleSize, spec.decayRate, new Random())
    warmupInput = ListBuffer.empty
    decayer = new Periodic(spec.decayPeriodType, spec.trainingPeriod, () => scorer.train(reservoir.getReservoir))
    retrainer = new Periodic(spec.trainingPeriodType, spec.decayPeriod, () => reservoir.advancePeriod())
  }

  override def flatMap(value: AggregatedRecordsWBaseline, out: Collector[(AggregatedRecordsWBaseline, Double)]): Unit = {
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
          out.collect((record,scorer.score(record)))
        }
        warmupInput.clear()
      }

      retrainer.runIfNecessary()
      decayer.runIfNecessary()
      reservoir.insert(value)
      out.collect((value,scorer.score(value)))
    }
  }
  override def close(): Unit = {}
}
