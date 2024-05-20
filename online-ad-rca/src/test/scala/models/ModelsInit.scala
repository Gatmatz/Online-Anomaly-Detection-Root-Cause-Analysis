package models

import org.scalatest.flatspec.AnyFlatSpec
import utils.Types.{ChildDimension, ParentDimension}

class ModelsInit extends AnyFlatSpec {

  "Dimension instance" should "be initialized" in {
    val dim: Dimension = Dimension(name="name", value="zisis", group="spatial", level = 0)
    println(dim)
  }

  "InputRecord instance" should "be initialized" in {
    val input_record: InputRecord = InputRecord(
      id = "fgt",
      timestamp = "1998-01-01T22:07:58",
      value = 234.0f,
      dimensions = Map("sm_type" -> Dimension("sm_type", "OVERNIGHT", "delivery", 1)),
      dimensions_hierarchy = Map[ChildDimension, ParentDimension]()
    )
    print(input_record)
  }
}
