package utils.count

import java.util
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
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

  private var counts: util.HashMap[Int, Double] = new util.HashMap[Int, Double]()

  private var totalCount: Double = 0
  private var prevEpochMaxEvicted: Double = 0

  def multiplyAllCounts(by: Double): Unit = {
    decayFactor = decayFactor / by

    if (decayFactor > DECAY_RESET_THRESHOLD)
      {
        resetDecayFactor()
      }

    if (counts.size > maxStableSize)
      {
        val entrySet = counts.entrySet()
        var a : util.ArrayList[util.Map.Entry[Integer, Double]] = new util.ArrayList[util.Map.Entry[Integer, Double]]()

        val entryList: Array[util.Map.Entry[Integer, Double]]= entrySet.toArray(new Array[util.Map.Entry[Integer, Double]](entrySet.size()))

        for (i <- entryList.indices)
          {
            a.add(entryList(i))
          }

        val toRemove = counts.size - maxStableSize

        var prevEpochMaxEvicted = Double.MinValue

        for (i <- 0 until toRemove) {
          val entry = a.get(i)
          counts.remove(entry.getKey)
          if (entry.getValue > prevEpochMaxEvicted) {
            prevEpochMaxEvicted = entry.getValue
          }
        }
      }
  }

  def getCounts: util.HashMap[Int, Double] = {
    resetDecayFactor()
    counts
  }

  private def resetDecayFactor(): Unit = {
    for (entry <- counts.entrySet) {
      val newValue: Double = entry.getValue / decayFactor
      counts.put(entry.getKey, newValue)
    }

    totalCount /= decayFactor

    decayFactor = 1
  }

  def observe(item: Int, count: Double): Unit = {
    val var_count = count * decayFactor

    var value : Double = counts.get(item)
    if (value == null)
      {
        value = prevEpochMaxEvicted + var_count
        totalCount = totalCount + value
      }
    else
      {
        value = value + var_count
        totalCount = totalCount + var_count
      }

    counts.put(item, value)

    if (value > DECAY_RESET_THRESHOLD && decayFactor > 1) {
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
    val ret: Double = counts.get(item)
    if (ret == null)
      return prevEpochMaxEvicted / decayFactor

    return ret/decayFactor
  }
}
