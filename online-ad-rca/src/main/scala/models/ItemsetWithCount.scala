package models

import scala.collection.mutable

class ItemsetWithCount(private val items: mutable.Set[Int], private val count: Double) {
  def getItems: mutable.Set[Int] = items

  def getCount: Double = count
}