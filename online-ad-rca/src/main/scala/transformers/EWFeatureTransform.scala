package transformers

import anomaly_detection.detectors.EWAppxPercentileOutlierClassifierSpec
import models.AggregatedRecordsWBaseline
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
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
                        ) extends KeyedProcessFunction[Int, AggregatedRecordsWBaseline, (AggregatedRecordsWBaseline, Double)] {

  private var reservoirState: ValueState[AdaptableDampedReservoir[AggregatedRecordsWBaseline]] = _
  private var scorerState: ValueState[MAD] = _
  private var warmupInput: ListBuffer[AggregatedRecordsWBaseline] = _
  private var tupleCount: Int = 0

  override def open(parameters: Configuration): Unit = {
    // Keep the ADR State
    val reservoirDescriptor = new ValueStateDescriptor[AdaptableDampedReservoir[AggregatedRecordsWBaseline]](
      "reservoirState",
      classOf[AdaptableDampedReservoir[AggregatedRecordsWBaseline]]
    )
    reservoirState = getRuntimeContext.getState(reservoirDescriptor)

    // Keep the Scorer State
    val scorerDescriptor = new ValueStateDescriptor[MAD](
      "scorerState",
      classOf[MAD]
    )

    scorerState = getRuntimeContext.getState(scorerDescriptor)


    warmupInput = ListBuffer[AggregatedRecordsWBaseline]()
    tupleCount = 0

  }

  override def processElement(value: AggregatedRecordsWBaseline, ctx: KeyedProcessFunction[Int, AggregatedRecordsWBaseline, (AggregatedRecordsWBaseline, Double)]#Context, out: Collector[(AggregatedRecordsWBaseline, Double)]): Unit = {
    tupleCount = tupleCount + 1

    // Fetch reservoir state
    var reservoir = reservoirState.value()
    if (reservoir == null)
      {
        reservoir = new AdaptableDampedReservoir[AggregatedRecordsWBaseline](spec.sampleSize, spec.decayRate, new Random())
      }

    // Fetch scorer state
    var scorer = scorerState.value()
    if (scorer == null)
    {
      scorer = new MAD()
    }

    if (tupleCount < spec.warmupCount)
      {
        warmupInput += value
        reservoir.insert(value)

        // Check retrainer
        if (tupleCount % spec.trainingPeriod == 0)
          reservoir.advancePeriod()

        // Check decayer
        if (tupleCount % spec.decayPeriod == 0)
          scorer.train(reservoir.getReservoir)
      }
    else
      {
        if (tupleCount == spec.warmupCount)
          {
            scorer.train(reservoir.getReservoir)
            for (record <- warmupInput) {
              out.collect((record,scorer.score(record)))
            }
            warmupInput.clear()
          }

        // Check retrainer
        if (tupleCount % spec.trainingPeriod.toInt == 0)
          reservoir.advancePeriod()

        // Check decayer
        if (tupleCount % spec.decayPeriod.toInt == 0)
          scorer.train(reservoir.getReservoir)

        reservoir.insert(value)
        out.collect((value,scorer.score(value)))
      }

    // Update ADR State
    reservoirState.update(reservoir)

    // Update MAD State
    scorerState.update(scorer)
  }
  override def close(): Unit = {}
}
