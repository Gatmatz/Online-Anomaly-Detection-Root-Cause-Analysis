package utils.encoder

import models.{Dimension, InputRecord}
import org.junit.Test
import org.junit.Assert.assertEquals
import utils.Types.{ChildDimension, ParentDimension}

import scala.collection.mutable.ListBuffer

class IntegerEncoderTest {

  @Test
  def testDimensions(): Unit = {
    val encoder = new IntegerEncoder()

    assertEquals(0, encoder.getDimensionInt("ca_city").getOrElse(-1))
    assertEquals(1, encoder.getDimensionInt("ca_county").getOrElse(-1))
    assertEquals(2, encoder.getDimensionInt("ca_state").getOrElse(-1))
    assertEquals(3, encoder.getDimensionInt("sm_code").getOrElse(-1))
  }

  @Test
  def testInsertion(): Unit = {
    val encoder = new IntegerEncoder()

    val input_record: InputRecord = InputRecord(
      id = "fgt",
      timestamp = "1998-01-01T22:07:58",
      value = 234.0f,
      dimensions = Map("sm_code" -> Dimension("sm_code", "OVERNIGHT", "delivery", 1),
                       "ca_state" -> Dimension("ca_state", "UK", "spatial", 1),
                       "ca_county" -> Dimension("ca_county", "London", "spatial", 2)),
      dimensions_hierarchy = Map[ChildDimension, ParentDimension]()
    )

    val encodings = ListBuffer[Int]()
    for ((key, value) <- input_record.dimensions) {
      val dimension_int: Int = encoder.getDimensionInt(key).getOrElse(-1)
      val encoding = encoder.getIntegerEncoding(dimension_int, value.value)
      encodings += encoding
    }

    assertEquals(0, encodings.head)
    assertEquals(1, encodings(1))
    assertEquals(2, encodings(2))

    val input_record_2: InputRecord = InputRecord(
      id = "fgt",
      timestamp = "1998-01-01T22:07:58",
      value = 234.0f,
      dimensions = Map("sm_code" -> Dimension("sm_code", "OVERNIGHT", "delivery", 1),
        "ca_state" -> Dimension("ca_state", "USA", "spatial", 1),
        "ca_county" -> Dimension("ca_county", "London", "spatial", 2)),
      dimensions_hierarchy = Map[ChildDimension, ParentDimension]()
    )

    val new_encodings = ListBuffer[Int]()
    for ((key, value) <- input_record_2.dimensions) {
      val dimension_int: Int = encoder.getDimensionInt(key).getOrElse(-1)
      val encoding = encoder.getIntegerEncoding(dimension_int, value.value)
      new_encodings += encoding
    }

    assertEquals(0, new_encodings.head)
    assertEquals(3, new_encodings(1))
    assertEquals(2, new_encodings(2))
  }
}
