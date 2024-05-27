package utils.itemset

import models.ItemsetWithCount
import org.junit.Assert.assertEquals
import org.junit.Test
import utils.itemset.FPTree.StreamingFPGrowth

import scala.collection.mutable
import scala.util.Random

class StreamingFPGrowthTest {

  private def intIfy(txnStr: String): Set[Int] = {
    txnStr.split(", ").map(_.charAt(0).toInt).toSet
  }

  private def printItemsets(itemsets: List[ItemsetWithCount]): Unit = {
    val sortedItemsets = itemsets.sortBy(_.getItems.size)(Ordering[Int].reverse)
    sortedItemsets.foreach { i =>
      println(s"\ncount ${i.getCount}, size ${i.getItems.size}")
      i.getItems.foreach(item => println(item.toChar))
    }
  }

  @Test
  def simpleTest(): Unit = {
    val allTxns = List(
      intIfy("a, b, c"),
      intIfy("a, b")
    )

    val fp = new StreamingFPGrowth(support = .5)
    fp.buildTree(allTxns)
    var itemsets = fp.getItemsets

    assertEquals(7, itemsets.size)

    val newBatch = List(
      intIfy("c, d"),
      intIfy("a, d"),
      intIfy("a, d, e")
    )

    fp.insertTransactionsStreamingExact(newBatch)

    itemsets = fp.getItemsets

    assertEquals(6, itemsets.size)

  }

  @Test
  def testFPFromPaper(): Unit = {
    val allTxns = List(
      intIfy("a, b, c, f, l, m, o"),
      intIfy("f, a, c, d, g, i, m, p"),
      intIfy("b, f, h, j, o"),
      intIfy("b, c, k, s, p"),
      intIfy("a, f, c, e, l, p, m, n")
    )
    val fp = new StreamingFPGrowth(.2)
    fp.buildTree(allTxns)

    var itemsets = fp.getItemsets
    assertEquals(625, itemsets.size)

    val newBatch = List(
      intIfy("a, b, c, d, e"),
      intIfy("b, a, d, a, s, s"),
      intIfy("d, a, t, t, h, i, n, g"),
      intIfy("f, a, k, s, p, e")
    )

    val updatedTxns = allTxns ++ newBatch
    fp.insertTransactionsStreamingExact(newBatch)
    itemsets = fp.getItemsets

    assertEquals(797, itemsets.size)
  }

  @Test
  def stress(): Unit = {
    val fp = new StreamingFPGrowth(.001)
    val random = new Random(seed = 0)
    var cnt = 0

    var frequentItems = new mutable.HashMap[Int, Double]()
    while (cnt <= 1000) {
      val itemSetSize = random.nextInt(100)
      val itemSet = mutable.Set[Int]()
      for (_ <- 0 until itemSetSize) {
        itemSet.add(random.nextInt(100))
        frequentItems(cnt) = frequentItems.getOrElse(cnt, 0.0) + 1
      }

      fp.insertTransactionFalseNegative(itemSet.toSet)

      if (cnt % 20 == 0) {
        val toDecay = random.nextInt(frequentItems.size)
        for (_ <- 0 until toDecay) {
          frequentItems.remove(frequentItems.keySet.toSeq(random.nextInt(frequentItems.size)))
        }
        fp.decayAndResetFrequentItems(frequentItems, .95)
      }
      cnt += 1
    }
  }
}
