package anomaly_detection.detectors

import models.{AggregatedRecordsWBaseline, AnomalyEvent}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import utils.sample.AdaptableDampedReservoir

import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
 * Auxiliary class for the exponentially weighted approximate percentile-based streaming classifier from Macrobase.
 * This class is a KeyedProcessFunction that performs the AD detection by keeping state of an ADR reservoir(sampling).
 * The function accepts a Stream of tuples in the form of (AggregatedRecordsWBaseline, Score)
 * and emits tuples of (AggregatedRecordsWBaseline, Boolean)
 * where Boolean will be an indicator of whether the Record is an Anomaly or not.
 * @param spec the specification of the AD detection
 */
class EWAppxPercentileAuxiliary(spec: EWAppxPercentileOutlierClassifierSpec)
  extends KeyedProcessFunction[Int, (AggregatedRecordsWBaseline, Double), AnomalyEvent] {

  private var reservoirState: ValueState[AdaptableDampedReservoir[(AggregatedRecordsWBaseline, Double)]] = _
  private var thresholdState: ValueState[Double] = _

  private var warmupInput: ListBuffer[(AggregatedRecordsWBaseline, Double)] = _
  private var tupleCount: Int = 0

  override def open(parameters: Configuration): Unit = {
    // Keep the ADR State
    val reservoirDescriptor = new ValueStateDescriptor[AdaptableDampedReservoir[(AggregatedRecordsWBaseline, Double)]](
      "reservoirState",
      classOf[AdaptableDampedReservoir[(AggregatedRecordsWBaseline, Double)]]
    )
    reservoirState = getRuntimeContext.getState(reservoirDescriptor)

    val thresholdDescriptor = new ValueStateDescriptor[Double](
      "thresholdState",
      classOf[Double]
    )

    thresholdState = getRuntimeContext.getState(thresholdDescriptor)

    warmupInput = new ListBuffer[(AggregatedRecordsWBaseline, Double)]
    tupleCount = 0
  }

  private def updateThreshold(reservoir:AdaptableDampedReservoir[(AggregatedRecordsWBaseline, Double)], percentile: Double): Double = {
    val norms: List[(AggregatedRecordsWBaseline, Double)] = reservoir.getReservoir
    val sortedNorms = norms.sortBy(_._2)
    val index = (percentile * norms.size).toInt
    val currentThreshold = sortedNorms(index)._2
    currentThreshold
  }

  override def processElement(value: (AggregatedRecordsWBaseline, Double), ctx: KeyedProcessFunction[Int, (AggregatedRecordsWBaseline, Double), AnomalyEvent]#Context, out: Collector[AnomalyEvent]): Unit = {
    tupleCount = tupleCount + 1
    // Fetch reservoir state
    var reservoir = reservoirState.value()
    if (reservoir == null)
    {
      reservoir = new AdaptableDampedReservoir[(AggregatedRecordsWBaseline, Double)](spec.sampleSize, spec.decayRate, new Random(seed = 0))
    }

    // Fetch threshold state
    var currentThreshold = thresholdState.value()
    if (currentThreshold == null)
    {
      currentThreshold = 0
    }

    if (tupleCount < spec.warmupCount)
      {
        warmupInput += value
        reservoir.insert(value)

        // Check reservoir decayer
        if (tupleCount % (spec.decayPeriod + 1) == 0)
          reservoir.advancePeriod()

        // Check threshold updater
        if (tupleCount % (spec.trainingPeriod + 1) == 0)
          currentThreshold = updateThreshold(reservoir, spec.percentile)
      }
    else
      {
        if (tupleCount == spec.warmupCount)
          {
            currentThreshold = updateThreshold(reservoir, spec.percentile)
            for (record <- warmupInput) {
              val isAnomaly: Boolean = value._2 > currentThreshold
              out.collect(AnomalyEvent(record._1, isAnomaly))
            }
            warmupInput.clear
          }

        reservoir.insert(value)
        val isAnomaly: Boolean = value._2 > currentThreshold
        out.collect(AnomalyEvent(value._1, isAnomaly))
      }

    // Update ADR State
    reservoirState.update(reservoir)

    // Update Threshold State
    thresholdState.update(currentThreshold)
  }
}
