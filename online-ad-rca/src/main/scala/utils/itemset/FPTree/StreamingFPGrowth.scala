package utils.itemset.FPTree

import models.ItemsetWithCount

import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Driver class for FPGrowth algorithm on a streaming environment.
 * @param support the minimum support required
 */
class StreamingFPGrowth(support: Double) extends Serializable {

  val fp: StreamingFPTree = new StreamingFPTree()
  var needsRestructure: Boolean = false
  var startedStreaming: Boolean = false


  def insertTransactionsStreamingExact(transactions: util.List[util.Set[Int]]): Unit = {
    needsRestructure = true

    fp.insertTransactions(transactions, streaming = true, filterExistingFrequentItemsOnly = false)
  }

  def insertTransactionFalseNegative(transaction: util.Set[Int]): Unit = {
    needsRestructure = true

    fp.insertTransaction(transaction, streaming = true, filterExistingFrequentItemsOnly = true)
  }

  def restructureTree(itemsToDelete: util.Set[Int]): Unit = {
    needsRestructure = false

    fp.deleteItems(itemsToDelete)
    fp.updateFrequentItemOrder()
    fp.sortByNewOrder()
  }

  def buildTree(transactions: util.List[util.Set[Int]]): Unit = {
    if (startedStreaming)
    {
      throw new Exception("Can't build a tree based on an already streaming tree..")
    }

    val countRequiredForSupport: Int = (support * transactions.size).toInt

    fp.insertFrequentItems(transactions, countRequiredForSupport)
    fp.insertTransactions(transactions, streaming = false, filterExistingFrequentItemsOnly = false)
  }

  def decayAndResetFrequentItems(newFrequentItems: util.Map[Int, Double],
                                 decayRate: Double): Unit = {
    val frequentItemOrderScala: mutable.Set[Int] = fp.frequentItemOrder.keySet.asScala
    val newFrequentItemsScala = newFrequentItems.keySet.asScala
    val toRemove: Set[Int] = frequentItemOrderScala.diff(newFrequentItemsScala).toSet
    fp.frequentItemCounts = newFrequentItems
    fp.updateFrequentItemOrder()
    if (decayRate > 0) {
      fp.decayWeights(fp.root, 1 - decayRate)
    }
    restructureTree(toRemove.asJava)
  }

  def getCounts(targets: util.List[ItemsetWithCount]): util.List[ItemsetWithCount] = {
    if (needsRestructure)
      {
        restructureTree(null)
      }
    val ret: util.ArrayList[ItemsetWithCount] = new util.ArrayList[ItemsetWithCount]()
    targets.forEach { target =>
      ret.add(new ItemsetWithCount(target.getItems, fp.getSupport(target.getItems)))
    }
    ret
  }

  def getItemsets: util.List[ItemsetWithCount] = {
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
