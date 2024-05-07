package utils.sample

import scala.collection.mutable
import scala.util.Random

class AChao[T](capacity: Int,
               random: Random = new Random) extends Serializable {
  private var runningCount: Double = 0
  private val reservoir: mutable.ListBuffer[T] = mutable.ListBuffer()
  private val reservoirCapacity: Int = capacity
  private val overweightItems: mutable.PriorityQueue[OverweightRecord] = mutable.PriorityQueue.empty(Ordering[OverweightRecord])

  def this(capacity: Int) = this(capacity, new Random())
  def insert(record:T, weight:Double): Unit = {
    runningCount = runningCount + weight

    updateOverweightItems()

    if (reservoir.size < reservoirCapacity )
      {
        reservoir.append(record)
      }
    else
      {
        val pInsertion = reservoirCapacity * weight / runningCount

        if (pInsertion > 1)
          {
            overweightItems.enqueue(OverweightRecord(record, weight))
          }
        else if (random.nextDouble() < pInsertion)
          {
            reservoir.update(random.nextInt(reservoirCapacity), record)
          }
      }

  }

  private def updateOverweightItems(): Unit = {
    while (overweightItems.nonEmpty)
      {
        val ow = overweightItems.head
        if (reservoirCapacity * ow.weight / runningCount <= 1) {
          overweightItems.dequeue()
          insert(ow.record, ow.weight)
        }
        else
          {
            return
          }
      }
  }

  def decayWeights(decay: Double): Unit = {
    runningCount *= decay
    val updatedOverweightItems = overweightItems.map(i => OverweightRecord(i.record, i.weight * decay))
    overweightItems.clear()
    overweightItems ++= updatedOverweightItems
  }


  def getReservoir: List[T] = {
    updateOverweightItems()

    if (overweightItems.nonEmpty)
      {
        // Overweight items always make it in the sample
        val overweightList: List[T] = overweightItems.map(_.record).toList

        assert(overweightList.size <= reservoirCapacity)

        // Fill the return value with a sample of non-overweight elements
        val shuffledReservoir = Random.shuffle(reservoir)
        val remainingRecords = reservoirCapacity - overweightList.size
        overweightList ::: shuffledReservoir.take(remainingRecords).toList
      }
    else
      {
        reservoir.toList
      }
  }
  private case class OverweightRecord(record: T,
                                      weight: Double) extends Comparable[OverweightRecord] {
    override def compareTo(o: OverweightRecord): Int = weight.compareTo(o.weight)
  }
}
