package models

import java.util

class ItemsetWithCount(private val items: util.Set[Int],
                       private val count: Double) extends Serializable {
  def getItems: util.Set[Int] = items

  def getCount: Double = count
}
