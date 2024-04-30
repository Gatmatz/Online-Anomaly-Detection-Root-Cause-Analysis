package utils.encoder

import config.AppConfig
import utils.Types

import scala.collection.mutable

/**
 * Integer Encoding Class that encodes Dimensions to unique integers
 */
class IntegerEncoder {
  private val attributeDimensionNameMap: mutable.Map[Int, String] = {
    val DIMENSION_NAMES = AppConfig.InputStream.DIMENSION_NAMES
    val map = mutable.HashMap[Int, String]()
    for (i <- DIMENSION_NAMES.indices) {
      map.put(i, DIMENSION_NAMES(i))
    }
    map
  }
  private val integerEncoding: mutable.Map[Int, mutable.Map[Types.DimensionValue, Int]] = mutable.HashMap()
  private var nextKey: Int = 0 // The next available integer key for assignment

  /**
   * Function that reverse searches for the integer key of the given dimension.
   * @param dimension the name of the dimension
   * @return the integer key of the given dimension
   */
  def getDimensionInt(dimension: String): Option[Int] = {
    attributeDimensionNameMap.find { case (_, v) => v == dimension }.map { case (k, _) => k }
  }

  /**
   * Creates or retrieves an integer encoding for a given attribute.
   * @param dimension the dimension key of the attribute
   * @param attr the attribute of the dimension
   * @return an integer representation of the attribute
   */
  def getIntegerEncoding(dimension: Int, attr: Types.DimensionValue): Int = {
    // Check if the attribute is already given to encoder
    val dimensionMap = integerEncoding.getOrElseUpdate(dimension, mutable.HashMap())
    // Retrieve the encoding or create a new one
    val ret = dimensionMap.getOrElse(attr, {
      val newKey = nextKey
      nextKey += 1
      dimensionMap(attr) = newKey
      newKey
    })
    ret
  }
}
