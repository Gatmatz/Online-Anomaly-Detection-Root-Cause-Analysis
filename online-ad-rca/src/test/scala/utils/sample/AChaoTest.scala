package utils.sample

import org.junit.Assert._
import org.junit.Test

import scala.util.Random

class AChaoTest {
  @Test
  def simpleTest(): Unit = {
    val sample = Array(1, 2, 3, 4, 5, 6, 7)

    val r = new Random(0)
    val ac = new AChao[Int](2, r)

    sample.foreach(i => ac.insert(i, 1))

    assertEquals(2, ac.getReservoir.size)
    assertEquals(5, ac.getReservoir.head)
    assertEquals(4, ac.getReservoir(1))
  }

  @Test
  def testOverweightItems(): Unit = {
    val sample = Array(1, 2, 3, 4, 5, 6, 7)

    val r = new Random(0)
    val ac = new AChao[Int](2, r)

    sample.foreach(i => ac.insert(i, 1))

    assertEquals(2, ac.getReservoir.size)
    assertEquals(5, ac.getReservoir.head)
    assertEquals(4, ac.getReservoir(1))

    ac.decayWeights(0.1)
    ac.insert(100, 1000)

    assertEquals(2, ac.getReservoir.size)
    assertTrue(ac.getReservoir.contains(100))

    ac.decayWeights(0.00001)
    ac.insert(200, 1000)

    assertTrue(ac.getReservoir.contains(200))
  }

  @Test
  def testOverweightItemSequential(): Unit = {
    val sample = Array(1, 2, 3, 4, 5, 6, 7)

    val r = new Random(0)
    val ac = new AChao[Int](100, r)

    for (_ <- 0 until 100) {
      sample.foreach(i => ac.insert(i, 1))
    }

    ac.decayWeights(0.00001)
    ac.insert(100, 1)
    ac.insert(200, 1)
    ac.insert(300, 1)

    assertEquals(100, ac.getReservoir.size)
    assertTrue(ac.getReservoir.contains(100))

    ac.decayWeights(0.0000001)
    ac.insert(400, 1)

    assertTrue(ac.getReservoir.contains(400))
  }
}
