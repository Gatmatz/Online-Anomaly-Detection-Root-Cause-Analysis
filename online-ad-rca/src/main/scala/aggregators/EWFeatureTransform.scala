package aggregators
import anomaly_detection.detectors.EWAppxPercentileOutlierClassifierSpec
import models.{InputRecord, InputRecordWithNorm}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import utils.Periodic
import utils.sample.AdaptableDampedReservoir
import utils.stats.MAD

import scala.collection.mutable.ListBuffer
import scala.util.Random

class EWFeatureTransform(spec: EWAppxPercentileOutlierClassifierSpec
                        ) extends RichFlatMapFunction[InputRecord, InputRecordWithNorm] {

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

  override def flatMap(value: InputRecord, out: Collector[InputRecordWithNorm]): Unit = {
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
          out.collect(new InputRecordWithNorm(record, scorer.score(record)))
        }
        warmupInput.clear()
      }

      retrainer.runIfNecessary()
      decayer.runIfNecessary()
      reservoir.insert(value)
      out.collect(new InputRecordWithNorm(value,scorer.score(value)))
    }
  }

  override def close(): Unit = {}
}
