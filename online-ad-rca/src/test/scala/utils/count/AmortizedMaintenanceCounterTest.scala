package utils.count

import org.junit.Assert.assertEquals
import org.junit.Test

import scala.collection.mutable
import scala.util.Random

class AmortizedMaintenanceCounterTest {
  @Test
  def simpleAMCTest(): Unit = {
    val ss = new AmortizedMaintenanceCounter(10)
    ss.observe(1)
    ss.observe(1)
    ss.observe(1)
    ss.observe(2)
    ss.observe(3)
    ss.observe(1)
    ss.observe(3)
    ss.observe(2)
    ss.observe(3)

    assertEquals(4, ss.getCount(1), 0)
    assertEquals(2, ss.getCount(2), 0)
    assertEquals(3, ss.getCount(3), 0)
  }

  @Test
  def overflowAMCTest(): Unit = {
    val ss = new AmortizedMaintenanceCounter(10)

    for (i <- 0 until 10) {
      ss.observe(i)
      assertEquals(1, ss.getCount(i), 0)
    }

    ss.observe(10)
    assertEquals(1, ss.getCount(10), 0)
  }

  @Test
  def decayTest(): Unit = {
    val N = 1000
    val ITEMS = 100
    val DECAY = 0.5
    val CAPACITY = 15
    val EPSILON = 1.0 / CAPACITY

    val ss = new AmortizedMaintenanceCounter(CAPACITY)

    val r = new Random(0)

    val trueCnt = mutable.Map[Int, Double]()

    for (i <- 0 until N) {
      val item = r.nextInt(ITEMS)
      ss.observe(item)

      trueCnt.update(item, trueCnt.getOrElse(item, 0.0) + 1)

      if (i % 10 == 0) {
        ss.multiplyAllCounts(DECAY)
        trueCnt.transform((i, v) => v * DECAY)
      }
    }

    val cnts = ss.getCounts

    cnts.foreach {
      case (key, value) => assertEquals(trueCnt.getOrElse(key, 0.0), value, N * EPSILON)
    }

    val key = cnts.keysIterator.next()
    assertEquals(cnts(key), ss.getCount(key), 1e-10)
  }
}
