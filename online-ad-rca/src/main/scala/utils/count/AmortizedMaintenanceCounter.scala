package utils.count

import scala.collection.mutable

/**
 * Maintains probabilistic heavy-hitters:
 * - Item counts are overreported.
 * - Once we have seen 1/threshold items, size is >= 1/threshold items.
 *
 * This is similar to SpaceSaving but with:
 * - O(1) update and O(items*log(k)) maintenance
 * (normally O(log(k)) update)
 * - unlimited space overhead within an epoch (normally O(k))
 *
 * Basic idea:
 * Store min count from previous epoch
 * Item in table? Increment
 * Item not in table? Min + count
 *
 * End of epoch:
 * 1.) decay counts
 * 2.) record new min
 * 3.) compute 1/thresh highest counts
 * 4.) discard lower items, updating min if necessary
 *
 *
 *
 * @param maxStableSize Counter's max size
 */
class AmortizedMaintenanceCounter(maxStableSize: Int) extends Serializable
{
  private var decayFactor: Double = 1
  private val DECAY_RESET_THRESHOLD: Double = Double.MaxValue * .5

  private var counts: mutable.HashMap[Int, Double] = mutable.HashMap.empty[Int, Double]

  private var totalCount: Double = 0
  private var prevEpochMaxEvicted: Double = 0

  def multiplyAllCounts(by: Double): Unit = {
    decayFactor /= by

    if (decayFactor > DECAY_RESET_THRESHOLD)
      {
        resetDecayFactor()
      }

    if (counts.size > maxStableSize)
      {
        val a = counts.toList.sortBy(_._2)

        val toRemove = counts.size - maxStableSize

        var prevEpochMaxEvicted = Double.MinValue

        for (i <- 0 until toRemove) {
          val entry = a(i)
          counts.remove(entry._1)
          if (entry._2 > prevEpochMaxEvicted) {
            prevEpochMaxEvicted = entry._2
          }
        }
      }
  }

  def getCounts: mutable.HashMap[Int, Double] = {
    resetDecayFactor()
    counts
  }

  private def resetDecayFactor(): Unit = {
    for ((key, value) <- counts) {
      val newValue = value / decayFactor
      counts.put(key, newValue)
    }

    totalCount /= decayFactor

    decayFactor = 1
  }

  def observe(item: Int, count: Double): Unit = {
    var var_count = count * decayFactor

    counts.get(item) match {
      case Some(value) =>
        val newValue = value + var_count
        counts.put(item, newValue)
        totalCount += var_count
      case None =>
        val newValue = prevEpochMaxEvicted + var_count
        counts.put(item, newValue)
        totalCount += newValue
    }

    if (counts(item) > DECAY_RESET_THRESHOLD && decayFactor > 1) {
      resetDecayFactor()
    }
  }

  def observe(item: Int): Unit = {
    observe(item, 1)
  }
  def observe(items: List[Int]): Unit = {
    for (item <- items) {
      observe(item)
    }
  }
  def getTotalCount: Double = {
    totalCount / decayFactor
  }

  def getCount(item: Int): Double = {
    counts.get(item) match {
      case Some(ret) => ret / decayFactor
      case None => prevEpochMaxEvicted / decayFactor
    }
  }
}
