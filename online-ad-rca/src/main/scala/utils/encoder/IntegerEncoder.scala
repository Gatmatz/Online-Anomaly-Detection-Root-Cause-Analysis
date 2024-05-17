package utils.encoder

import config.AppConfig
import models.Dimension
import utils.Types

import scala.collection.mutable

/**
 * Integer Encoding Class that encodes Dimensions to unique integers.
 */
class IntegerEncoder extends Serializable {
  private val integerEncoding: mutable.Map[Types.DimensionName, mutable.Map[Types.DimensionValue, Int]] = mutable.HashMap()
  private var nextKey: Int = 0 // The next available integer key for assignment
  private val integerToDimension: mutable.Map[Int, Types.DimensionName] = mutable.HashMap()
  /**
   * Creates or retrieves an integer encoding for a given attribute.
   * @param dimension the dimension key of the attribute
   * @param attr the attribute of the dimension
   * @return an integer representation of the attribute
   */
  def getIntegerEncoding(dimension: Dimension): Int = {
    // Check if the attribute is already given to encoder
    val dimensionMap = integerEncoding.getOrElseUpdate(dimension.name, mutable.HashMap())
    // Retrieve the encoding or create a new one
    val ret = dimensionMap.getOrElse(dimension.value, {
      val newKey = nextKey
      nextKey += 1
      integerToDimension(newKey) = dimension.name
      dimensionMap(dimension.value) = newKey
      newKey
    })
    ret
  }

  def getAttribute(encodedAttr: Int): Dimension = {
    val dimensionName: Types.DimensionName = integerToDimension.get(encodedAttr).orNull
    val dimensionMap = integerEncoding.getOrElse(dimensionName, null)

    var attribute: Types.DimensionValue = null
    for ((key, value) <- dimensionMap) {
      if (value == encodedAttr) {
        attribute = key
      }
    }

//    val group: Types.DimensionGroup = AppConfig.InputStream.DIMENSION_DEFINITIONS.getConfig(dimensionName).getString("group")
//    val level: Types.DimensionLevel = AppConfig.InputStream.DIMENSION_LEVELS(dimensionName)

//    Dimension(dimensionName, attribute, group, level)
      Dimension(dimensionName, attribute, "column", 1)
  }
}
