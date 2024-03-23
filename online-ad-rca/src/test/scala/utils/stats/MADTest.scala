import models.InputRecord
import org.apache.commons.math3.linear.ArrayRealVector
import org.junit.Assert.assertEquals
import org.junit.Test
import utils.stats.MAD

import java.util
import scala.collection.mutable.ListBuffer

class MADTest {
  @Test
  def simpleTest(): Unit = {
    val m: MAD = new MAD()

    val data: ListBuffer[InputRecord] = ListBuffer()
    for (i <- 0 until 100) {
      val sample: Array[Double] = Array(i.toDouble)
//      data += new InputRecord(new util.ArrayList[Double](), new ArrayRealVector(sample))
    }

    m.train(data.toList)
    assertEquals(1.98, m.score(data.head), 1e-5)
    assertEquals(1.98, m.score(data.last), 1e-5)
    assertEquals(0.02, m.score(data(50)), 1e-5)
  }

  @Test
  def zeroMADTest(): Unit = {
    val m: MAD = new MAD()

    val data: ListBuffer[InputRecord] = ListBuffer()
    for (i <- 0 until 30) {
      val sample: Array[Double] = if (i == 0 || i >= 28) Array(5.0) else Array(10.0)
//      data += new InputRecord(new java.util.ArrayList[Double](), new ArrayRealVector(sample))
    }

    m.train(data.toList)
    assertEquals(27, m.score(data(0)), 0)
    assertEquals(0, m.score(data(2)), 0)
  }

  @Test
  def zScoreTest(): Unit = {
    val m: MAD = new MAD()

    val data: ListBuffer[InputRecord] = ListBuffer()
    for (i <- 0 until 10) {
      val sample: Array[Double] = Array(i.toDouble)
//      data += new InputRecord(new util.ArrayList[Double](), new ArrayRealVector(sample))
    }

    val sample: Array[Double] = Array(20.0)
//    data += new InputRecord(new java.util.ArrayList[Double](), new ArrayRealVector(sample))

    m.train(data.toList)
    assertEquals(5.0, m.score(data.last), 1e-5)
    assertEquals(5.0 / 1.4826,
      m.getZScoreEquivalent(m.score(data.last)),
      1e-1)
  }
}
