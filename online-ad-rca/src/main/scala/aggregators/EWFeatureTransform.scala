package aggregators
import models.{AggregatedRecordsWBaseline, InputRecord, InputRecordWithNorm}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import utils.sample.AdaptableDampedReservoir
import utils.stats.MAD

import scala.collection.mutable.ListBuffer
import scala.util.Random

class EWFeatureTransform(sampleSize: Int,
                         decayRate: Double,
                         decayPeriod: Double,
                         trainingPeriod: Double,
                         warmupCount: Int) extends RichFlatMapFunction[InputRecord, InputRecordWithNorm] {

  private var tupleCount: Int = 0
  private val random: Random = new Random()

  private var scorer: MAD = _
  private var reservoir: AdaptableDampedReservoir[InputRecord] = _
  private var warmupInput: ListBuffer[InputRecord] = _
  private var retrainer: Periodic = _
  private var decayer: Periodic = _

  override def open(parameters: Configuration): Unit = {
    scorer = new MAD()
    reservoir = new AdaptableDampedReservoir[InputRecord](sampleSize, decayRate, random)
    warmupInput = ListBuffer.empty
    retrainer = new Periodic(decayPeriod, reservoir.advancePeriod())
    decayer = new Periodic(trainingPeriod, () => scorer.train(reservoir.getReservoir))
  }

  override def flatMap(value: InputRecord, out: Collector[InputRecordWithNorm]): Unit = {
    tupleCount += 1

    if (tupleCount < warmupCount)
    {
      warmupInput += value
      reservoir.insert(value)
      retrainer.runIfNecessary()
      decayer.runIfNecessary()
    }
    else
    {
      if (tupleCount == warmupCount)
      {
        scorer.train(reservoir.getReservoir)
        for (di <- warmupInput)
        {
          out.collect(new InputRecordWithNorm(di, scorer.score(di)))
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
