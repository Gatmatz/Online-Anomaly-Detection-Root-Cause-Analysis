package utils.itemset.FPTree

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}

class StreamingFPTree {
  // Create the root
  private var root: FPTreeNode = new FPTreeNode(-1, null,0)

  private var frequentItemCounts: mutable.Map[Int, Double] = mutable.Map.empty[Int, Double]

  // Used to calculate the order
  private var frequentItemOrder: mutable.Map[Int, Int] = mutable.Map.empty[Int, Int]

  protected var nodeHeaders: mutable.Map[Int, FPTreeNode] = mutable.Map.empty[Int, FPTreeNode]

  protected var leafNodes: mutable.Set[FPTreeNode] = mutable.Set.empty[FPTreeNode]

  protected var sortedNodes: mutable.Set[FPTreeNode] = mutable.Set.empty[FPTreeNode]


  /*******************************************************
   * TREE DEBUGGING FUNCTIONS
   *******************************************************/

  /**
   * Debug function for Frequent Item Counts
   */
  private def debugFrequentItemCounts(): Unit = {
    println("Frequent Item Counts:")
    frequentItemCounts.foreach { case (key, value) =>
      println(s"$key $value")
    }
  }

  /**
   * Debug function for Frequent Item Order
   */
  private def debugFrequentItemOrder(): Unit = {
    println("Frequent Item Order:")
    frequentItemOrder.foreach { case (key, value) =>
      println(s"$key $value")
    }
  }

  /**
   * General debug function that prints out the frequent item counts with their order and
   * then proceeds to print out the tree by walking it
   */
  private def debugTree(): Unit = {
    debugFrequentItemCounts()
    debugFrequentItemOrder()
    walkTree(root,1)
  }

  /**
   * Function that walks a tree starting from a specific node and a specific depth
   * @param start the starting point of the traversal
   * @param treeDepth the current tree depth
   */
  private def walkTree(start: FPTreeNode, treeDepth: Int): Unit = {
    val indentation = "\t" * treeDepth
    println("{} node: {}, count: {}, sorted: {}", indentation, start.getItem, start.getCount, sortedNodes.contains(start))

    if (start.getChildren != null)
      {
        start.getChildren.foreach { child =>
          walkTree(child, treeDepth + 1)
      }
  }

  def decayWeights(start: FPTreeNode, decayWeight: Double): Unit = {
    if (start == root)
      {
        for ((item, count) <- frequentItemCounts) {
          frequentItemCounts.put(item, count * decayWeight)
        }
      }

    start.count *= decayWeight
    if (start.getChildren != null)
      {
        for (child <- start.getChildren) {
          decayWeights(child, decayWeight)
        }
      }
  }

    def getSupport(pattern: ListBuffer[Int]): Int = {
      for (i <- pattern) {
        if (!frequentItemCounts.contains(i))
          return 0
      }

      val plist: ListBuffer[Int] = pattern.clone()

      plist.sortWith((i1, i2) => frequentItemOrder(i1).compareTo(frequentItemOrder(i2)) < 0)

      var count: Int = 0
      var pathHead: FPTreeNode = nodeHeaders(plist.head)
      while(pathHead != null)
        {
          var curNode: FPTreeNode = pathHead
          var itemsToFind: Int = plist.size
          breakable
          {
            while (curNode != null)
            {
              if (pattern.contains(curNode.getItem))
                itemsToFind -= 1

              if (itemsToFind == 0)
              {
                count += pathHead.count
                break
              }

              curNode = curNode.getParent
            }
          }
          pathHead = pathHead.getNextLink
        }
      count
    }
  }

  def insertFrequentItems(transactions: ListBuffer[mutable.Set[Int]], countRequiredForSupport: Int): Unit = {

  }
}
