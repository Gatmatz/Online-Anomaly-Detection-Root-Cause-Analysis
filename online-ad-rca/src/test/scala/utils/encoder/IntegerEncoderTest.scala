package utils.encoder

import models.{Dimension, InputRecord}
import org.junit.Test
import org.junit.Assert.assertEquals
import utils.Types.{ChildDimension, ParentDimension}

import scala.collection.mutable.ListBuffer

class IntegerEncoderTest {

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
      val encoding = encoder.getIntegerEncoding(value)
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
      val encoding = encoder.getIntegerEncoding(value)
      new_encodings += encoding
    }

    assertEquals(0, new_encodings.head)
    assertEquals(3, new_encodings(1))
    assertEquals(2, new_encodings(2))


    // Retrieve dimensions
    val retrievedDimension = encoder.getAttribute(0)
    assertEquals("sm_code", retrievedDimension.name)
    assertEquals("OVERNIGHT", retrievedDimension.value)
    assertEquals("delivery", retrievedDimension.group)
    assertEquals(1, retrievedDimension.level)
  }
}
