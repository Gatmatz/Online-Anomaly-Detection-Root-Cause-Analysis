package models

class ItemsetWithCount(private val items: Set[Int], private val count: Double) {
  def getItems: Set[Int] = items

  def getCount: Double = count
}
