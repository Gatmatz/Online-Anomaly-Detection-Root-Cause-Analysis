package utils.itemset

import models.ItemsetWithCount
import org.junit.Assert.assertEquals
import org.junit.Test
import utils.itemset.FPTree.StreamingFPGrowth

import java.util
import java.util.Random
import scala.collection.JavaConverters._
import scala.collection.mutable
class StreamingFPGrowthTest {

  private def intIfy(txnStr: String): util.Set[Int] = {
    txnStr.split(", ").map(_.charAt(0).toInt).toSet.asJava
  }

  private def printItemsets(itemsets: util.List[ItemsetWithCount]): Unit = {
    itemsets.forEach { itemset =>
      println(itemset.getItems)
    }
  }

  @Test
  def simpleTest(): Unit = {
    val allTxns: util.List[util.Set[Int]] = new util.ArrayList[util.Set[Int]]()
    allTxns.add(intIfy("a, b, c"))
    allTxns.add(intIfy("a, b"))

    val fp = new StreamingFPGrowth(support = .5)
    fp.buildTree(allTxns)
    var itemsets = fp.getItemsets
    assertEquals(7, itemsets.size)

    val newBatch: util.List[util.Set[Int]] = new util.ArrayList[util.Set[Int]]()
    newBatch.add(intIfy("c, d"))
    newBatch.add(intIfy("a, d"))
    newBatch.add(intIfy("a, d, e"))

    fp.insertTransactionsStreamingExact(newBatch)

    itemsets = fp.getItemsets

    printItemsets(itemsets)

    assertEquals(6, itemsets.size)
  }

  @Test
  def testFPFromPaper(): Unit = {
    val allTxns: util.List[util.Set[Int]] = new util.ArrayList[util.Set[Int]]()
    allTxns.add(intIfy("a, b, c, f, l, m, o"))
    allTxns.add(intIfy("f, a, c, d, g, i, m, p"))
    allTxns.add(intIfy("b, f, h, j, o"))
    allTxns.add(intIfy("b, c, k, s, p"))
    allTxns.add(intIfy("a, f, c, e, l, p, m, n"))

    val fp = new StreamingFPGrowth(.2)
    fp.buildTree(allTxns)

    var itemsets = fp.getItemsets
    assertEquals(625, itemsets.size)

    val newBatch: util.List[util.Set[Int]] = new util.ArrayList[util.Set[Int]]()
    newBatch.add(intIfy("a, b, c, d, e"))
    newBatch.add(intIfy("b, a, d, a, s, s,"))
    newBatch.add(intIfy("d, a, t, t, h, i, n, g"))
    newBatch.add(intIfy("f, a, k, s, p, e"))

    fp.insertTransactionsStreamingExact(newBatch)
    itemsets = fp.getItemsets

    assertEquals(797, itemsets.size)
  }

  @Test
  def stress(): Unit = {
    val fp = new StreamingFPGrowth(.001)
    val random = new Random(0)
    var cnt = 0

    val frequentItems: util.Map[Int, Double] = new util.HashMap[Int, Double]()
    for (cnt <- 0 until 1000)
      {
        val itemSetSize: Int = random.nextInt(100)
        val itemSet: util.Set[Int] = new util.HashSet[Int](itemSetSize)
        for (i <- 0 until itemSetSize)
          {
            itemSet.add(random.nextInt(100))
            frequentItems.compute(i, (k: Int, v: Double) => if (v == null) 1
            else v + 1)
          }

        fp.insertTransactionFalseNegative(itemSet)

        if (cnt % 20 == 0)
          {
            val toDecay: Int = random.nextInt(frequentItems.size)
            for (i <- 0 until toDecay)
              {
                val keySet: util.Set[Int] = frequentItems.keySet()
                val keyArray: util.ArrayList[Int] = new util.ArrayList[Int](keySet)
                val randomIndex = random.nextInt(frequentItems.size())
                frequentItems.remove(keyArray.get(randomIndex))
              }

            fp.decayAndResetFrequentItems(frequentItems, .95)
          }
      }

  }
}
