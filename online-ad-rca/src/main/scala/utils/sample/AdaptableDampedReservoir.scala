package utils.sample

import scala.util.Random

/**
 * Keeps an exponentially weighted sample with specified bias parameter
 * N.B. The current period is advanced explicitly.
 */
class AdaptableDampedReservoir[T](capacity:Int, bias:Double, random: Random = new Random()) extends AChao[T](capacity, random) {
  require(bias >= 0 && bias < 1, "Bias parameter must be between 0 and 1.")
  def this(capacity: Int, bias: Double) = this(capacity, bias, new Random())

  def advancePeriod(): Unit = {
    advancePeriod(1)
  }
  def advancePeriod(numPeriods: Int): Unit = {
    decayWeights(math.pow(1 - bias, numPeriods))
  }

  override def insert(record: T, weight: Double): Unit = super.insert(record, 1)
}
