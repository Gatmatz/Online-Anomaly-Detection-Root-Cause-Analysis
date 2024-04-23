package utils.itemset.FPTree

import models.ItemsetWithCount

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class StreamingFPGrowth(support: Double) {

  val fp: StreamingFPTree = new StreamingFPTree()
  var needsRestructure: Boolean = false
  var startedStreaming: Boolean = false


  def insertTransactionsStreamingExact(transactions: List[Set[Int]]): Unit = {
    needsRestructure = true

    fp.insertTransactions(transactions, streaming = true, filterExistingFrequentItemsOnly = false)
  }

  def insertTransactionFalseNegative(transaction: Set[Int]): Unit = {
    needsRestructure = true

    fp.insertTransaction(transaction, streaming = true, filterExistingFrequentItemsOnly = true)
  }

  def restructureTree(itemsToDelete: Set[Int]): Unit = {
    needsRestructure = false

    fp.deleteItems(itemsToDelete)
    fp.updateFrequentItemOrder()
    fp.sortByNewOrder()
  }

  def buildTree(transactions: List[Set[Int]]): Unit = {
    if (startedStreaming)
    {
      throw new Exception("Can't build a tree based on an already streaming tree..")
    }

    val countRequiredForSupport: Int = (support * transactions.size).toInt

    fp.insertFrequentItems(transactions, countRequiredForSupport)
    fp.insertTransactions(transactions, streaming = false, filterExistingFrequentItemsOnly = false)
  }

  def decayAndResetFrequentItems(newFrequentItems: mutable.Map[Int, Double],
                                 decayRate: Double): Unit = {
    val toRemove: Set[Int] = fp.frequentItemOrder.keySet.diff(newFrequentItems.keySet).toSet
    fp.frequentItemCounts = newFrequentItems
    fp.updateFrequentItemOrder()
    if (decayRate > 0) {
      fp.decayWeights(fp.root, 1 - decayRate)
    }
    restructureTree(toRemove)
  }

  def getCounts(targets: List[ItemsetWithCount]): List[ItemsetWithCount] = {
    if (needsRestructure)
      {
        restructureTree(null)
      }

    val ret = ListBuffer[ItemsetWithCount]()
    for (target <- targets) {
      ret += new ItemsetWithCount(target.getItems, fp.getSupport(target.getItems.toList))
    }
    ret.toList
  }

  def getItemsets: List[ItemsetWithCount] = {
    if (needsRestructure)
      {
        restructureTree(null)
      }

    val itemset = fp.mineItemsets((fp.root.getCount * support).toInt)
    itemset
  }

  def printTreeDebug(): Unit = {
    fp.debugTree()
  }
}
