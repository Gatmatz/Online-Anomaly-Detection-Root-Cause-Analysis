import models.{Dimension, InputRecord}
import org.apache.commons.math3.linear.ArrayRealVector
import org.junit.Assert.assertEquals
import org.junit.Test
import utils.Types.{ChildDimension, ParentDimension}
import utils.stats.MAD

import scala.collection.mutable.ListBuffer

class MADTest {
  @Test
  def simpleTest(): Unit = {
    val m: MAD = new MAD()

    val data: ListBuffer[InputRecord] = ListBuffer.empty[InputRecord]
    for (i <- 0 until 100) {
      val input_record: InputRecord = InputRecord(
        id = "simpleTest",
        timestamp = "1998-01-01T22:07:58",
        value = i,
        dimensions = Map("sm_type" -> Dimension("sm_type", "OVERNIGHT", "delivery", 1)),
        dimensions_hierarchy = Map[ChildDimension, ParentDimension]()
      )
      data += input_record
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
      val sample: Double = if (i == 0 || i >= 28) 5.0 else 10.0
      val input_record: InputRecord = InputRecord(
        id = "zeroMADTest",
        timestamp = "1998-01-01T22:07:58",
        value = sample,
        dimensions = Map("sm_type" -> Dimension("sm_type", "OVERNIGHT", "delivery", 1)),
        dimensions_hierarchy = Map[ChildDimension, ParentDimension]()
      )
      data += input_record
    }

    m.train(data.toList)
    assertEquals(27, m.score(data.head), 0)
    assertEquals(0, m.score(data(2)), 0)
  }

  @Test
  def zScoreTest(): Unit = {
    val m: MAD = new MAD()

    val data: ListBuffer[InputRecord] = ListBuffer()
    for (i <- 0 until 10) {
      val sample: Double = i.toDouble
      val input_record: InputRecord = InputRecord(
        id = "zScoreTest",
        timestamp = "1998-01-01T22:07:58",
        value = sample,
        dimensions = Map("sm_type" -> Dimension("sm_type", "OVERNIGHT", "delivery", 1)),
        dimensions_hierarchy = Map[ChildDimension, ParentDimension]()
      )
      data += input_record
    }

    val sample: Double = 20.0
    val input_record: InputRecord = InputRecord(
      id = "zScoreTest",
      timestamp = "1998-01-01T22:07:58",
      value = sample,
      dimensions = Map("sm_type" -> Dimension("sm_type", "OVERNIGHT", "delivery", 1)),
      dimensions_hierarchy = Map[ChildDimension, ParentDimension]()
    )
    data += input_record

    m.train(data.toList)
    assertEquals(5.0, m.score(data.last), 1e-5)
    assertEquals(5.0 / 1.4826,
      m.getZScoreEquivalent(m.score(data.last)),
      1e-1)
  }
}
