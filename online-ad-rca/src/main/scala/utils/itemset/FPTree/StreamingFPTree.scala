package utils.itemset.FPTree

import models.ItemsetWithCount
import org.apache.commons.compress.utils.Sets

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.language.postfixOps
import scala.util.control.Breaks.{break, breakable}

class StreamingFPTree {
  // Create the root
  private val root: FPTreeNode = new FPTreeNode(-1, null, 0)

  private val frequentItemCounts: mutable.Map[Int, Double] = mutable.Map.empty[Int, Double]

  // Used to calculate the order
  private val frequentItemOrder: mutable.Map[Int, Int] = mutable.Map.empty[Int, Int]

  var nodeHeaders: mutable.Map[Int, FPTreeNode] = mutable.Map.empty[Int, FPTreeNode]

  var leafNodes: mutable.HashSet[FPTreeNode] = mutable.HashSet.empty[FPTreeNode]

  protected var sortedNodes: mutable.Set[FPTreeNode] = mutable.Set.empty[FPTreeNode]


  /** *****************************************************
   * TREE DEBUGGING FUNCTIONS
   * ******************************************************/

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
    walkTree(root, 1)
  }

  /**
   * Function that traverses a tree starting from a specific node and a specific depth.
   * @param start     the starting point of the traversal
   * @param treeDepth the current tree depth
   */
  private def walkTree(start: FPTreeNode, treeDepth: Int): Unit = {
    val indentation = "\t" * treeDepth
    println("{} node: {}, count: {}, sorted: {}", indentation, start.getItem, start.getCount, sortedNodes.contains(start))

    if (start.getChildren != null) {
      start.getChildren.foreach { child =>
        walkTree(child, treeDepth + 1)
      }
    }
  }

  def decayWeights(start: FPTreeNode, decayWeight: Double): Unit = {
     if (start == root) {
       for ((item, count) <- frequentItemCounts) {
         frequentItemCounts.put(item, count * decayWeight)
       }
     }

      start.count *= decayWeight
      if (start.getChildren != null) {
        for (child <- start.getChildren) {
          decayWeights(child, decayWeight)
        }
      }
    }

  def getSupport(pattern: List[Int]): Int = {
    for (i <- pattern) {
      if (!frequentItemCounts.contains(i))
        return 0
    }

    val plist: List[Int] = pattern

    plist.sortWith((i1, i2) => frequentItemOrder(i1).compareTo(frequentItemOrder(i2)) < 0)

    var count: Int = 0
    var pathHead: FPTreeNode = nodeHeaders(plist.head)
    while (pathHead != null) {
      var curNode: FPTreeNode = pathHead
      var itemsToFind: Int = plist.size
      breakable {
        while (curNode != null) {
          if (pattern.contains(curNode.getItem))
            itemsToFind -= 1

          if (itemsToFind == 0) {
            count += pathHead.count.toInt
            break
          }

          curNode = curNode.getParent
        }
      }
      pathHead = pathHead.getNextLink
    }
    count
  }

  def insertFrequentItems(transactions: List[Set[Int]], countRequiredForSupport: Int): Unit = {
    val itemCounts: mutable.HashMap[Int, Double] = mutable.HashMap.empty[Int, Double]
    for (t <- transactions) {
      t.foreach(item =>
        itemCounts(item) += 1
      )
    }

    itemCounts.filter {
        case (_, value) => value >= countRequiredForSupport
      }
      .foreach {
        case (key, value) => frequentItemCounts.put(key, value)
      }


    // We have to materialize a canonical order so that items with equal counts
    // are consistently ordered when they are sorted during transaction insertion
    var sortedItemCounts = frequentItemCounts.toList
    sortedItemCounts = sortedItemCounts.sortBy(entry => frequentItemCounts(entry._1))

    // Populate the frequentItemOrder map
    sortedItemCounts.zipWithIndex.foreach { case ((key, _), index) =>
      frequentItemOrder(key) = index
    }
  }

  def deleteItems(itemsToDelete: Set[Int]): Unit = {
    if (itemsToDelete == null)
      {
        return
      }

    for (item <- itemsToDelete) {
      frequentItemCounts -= item
      frequentItemOrder -= item

      var nodeToDelete: FPTreeNode = nodeHeaders(item)

      while(nodeToDelete != null)
        {
          nodeToDelete.parent.removeChild(nodeToDelete)

          if (nodeToDelete.hasChildren)
            {
              nodeToDelete.parent.mergeChildren(nodeToDelete.getChildren)
            }

          leafNodes.remove(nodeToDelete)

          nodeToDelete = nodeToDelete.getNextLink
        }
      nodeHeaders.remove(item)
    }
  }

  def updateFrequentItemOrder(): Unit = {
    sortedNodes.clear()

    frequentItemOrder.clear()

    // We have to materialize a canonical order so that items with equal counts
    // are consistently ordered when they are sorted during transaction insertion
    var sortedItemCounts = frequentItemCounts.toList
    sortedItemCounts = sortedItemCounts.sortBy(entry => frequentItemCounts(entry._1))

    // Populate the frequentItemOrder map
    sortedItemCounts.zipWithIndex.foreach { case ((key, _), index) =>
      frequentItemOrder(key) = index
    }
  }

  def insertConditionalFrequentItems(patterns:List[ItemsetWithCount], countRequiredForSupport: Int): Unit = {
    val itemCounts: mutable.HashMap[Int, Double] = mutable.HashMap.empty[Int, Double]

    for (i <- patterns) {
      for (item <- i.getItems) {
        itemCounts.update(item, itemCounts.getOrElse(item, 0.0) + i.getCount)
      }
    }

    for ((key, value) <- itemCounts) {
      if (value >= countRequiredForSupport)
        frequentItemCounts.put(key, value)
    }

    updateFrequentItemOrder()
  }

  def sortTransaction(txn: List[Int]): Unit = {
    txn.sortBy(i => -frequentItemOrder.getOrElseUpdate(i, -i))
  }

  def reinsertBranch(pattern: Set[Int], count: Double, rootOfBranch: FPTreeNode): Unit = {
    val filtered: List[Int] = pattern.filter(frequentItemCounts.contains).toList
    sortTransaction(filtered)
    rootOfBranch.insertTransaction(filtered, count, 0)
  }

  def insertConditionalFrequentPatterns(patterns: List[ItemsetWithCount]): Unit = {
    for (is <- patterns) {
      reinsertBranch(is.getItems, is.getCount, root)
    }
  }

  def insertTransactions(transactions: List[Set[Int]], filterExistingFrequentItemsOnly: Boolean): Unit = {
    transactions.foreach(transaction => insertTransaction(transaction, filterExistingFrequentItemsOnly))
  }

  def insertTransaction(transaction: Set[Int], filterExistingFrequentItemsOnly: Boolean): Unit = {
    if (!filterExistingFrequentItemsOnly)
      {
        transaction.foreach(item => frequentItemCounts.update(item, frequentItemCounts.getOrElse(item, 0) + 1))
      }

    val filtered: List[Int] = transaction.filter(frequentItemCounts.contains).toList

    if (filtered.nonEmpty)
      {
        if (filterExistingFrequentItemsOnly)
          {
            filtered.foreach(item => frequentItemCounts.update(item, frequentItemCounts.getOrElse(item, 0 ) + 1))
          }

        sortTransaction(filtered)
        root.insertTransaction(filtered, 0, 1)
      }
  }

  def removeNodeFromHeaders(node:FPTreeNode): Unit = {
    leafNodes -= node

    if (node.getPrevLink == null)
      {
        assert(nodeHeaders(node.getItem) == node)
        nodeHeaders.put(node.getItem, node.getNextLink)
      }
    else
      {
        node.getPrevLink.setNextLink(node.getNextLink)
      }

    if (node.getNextLink != null)
      node.getNextLink.setPrevLink(node.getPrevLink)
  }

  def mineItemsets(supportCountRequired: Int): List[ItemsetWithCount] = {
    val singlePathItemsets: ListBuffer[ItemsetWithCount] = ListBuffer.empty[ItemsetWithCount]
    val branchingItemsets: ListBuffer[ItemsetWithCount] = ListBuffer.empty[ItemsetWithCount]

    // Mine single-path itemsets first
    var curNode: FPTreeNode = root
    var nodeOfBranching: FPTreeNode = null
    var singlePathNodes: Set[FPTreeNode] = Set.empty[FPTreeNode]

    breakable
    {
      while(true)
        {
          if (curNode.count < supportCountRequired)
              break()

          if (curNode.getChildren != null && curNode.getChildren.size > 1)
              nodeOfBranching = curNode
              break()

          if (curNode != root)
              singlePathNodes += curNode

          if (curNode.getChildren == null || curNode.getChildren.size == 0)
            break()
          else
            curNode = curNode.getChildren.head
        }
    }
    for (subset <- singlePathNodes.subsets()) {
      if (subset.isEmpty) {

      }
      else {
        var minSupportInSubset: Double = -1.0
        val items = mutable.Set[Int]()

        for (n <- subset) {
          items.add(n.getItem)

          if (minSupportInSubset == -1 || n.getCount < minSupportInSubset)
            minSupportInSubset = n.getCount
        }

        assert(minSupportInSubset >= supportCountRequired)
        singlePathItemsets += new ItemsetWithCount(items.toSet, minSupportInSubset)
      }
    }

    // the entire tree was a single path
    if (nodeOfBranching == null)
      return singlePathItemsets.toList

    // all of the items in the single path will have been mined now
    // due to the descending frequency count of the StreamingFPTree structure, so
    // we remove them from consideration in the rest
    // instead of destructively removing the nodes from NodeHeader table
    // which would be valid but would make mining non-idempotent, we
    // instead store the nodes to skip in a separate set
    var alreadyMinedItems = mutable.HashSet[Int]()
    for (node <- singlePathNodes) {
      alreadyMinedItems.add(node.getItem)
    }

    for ((headerKey, headerValue) <- nodeHeaders) {
      if (alreadyMinedItems.contains(headerKey) || frequentItemCounts.getOrElse(headerKey, 0) < supportCountRequired) {

      }
      else {
        // Add the singleton item set
        branchingItemsets += new ItemsetWithCount(Set(headerKey), frequentItemCounts(headerKey))

        val conditionalPatternBase = mutable.ListBuffer.empty[ItemsetWithCount]

        // Walk each "leaf" node
        var conditionalNode = headerValue
        while (conditionalNode != null) {
          val leafSupport = conditionalNode.getCount

          // Walk the tree up to the branch node
          val conditionalPattern = mutable.Set.empty[Int]
          var walkNode = conditionalNode.getParent
          while (walkNode != nodeOfBranching.getParent && walkNode != root) {
            conditionalPattern.add(walkNode.getItem)
            walkNode = walkNode.getParent
          }

          if (conditionalPattern.nonEmpty) {
            conditionalPatternBase += new ItemsetWithCount(conditionalPattern.toSet, leafSupport)
          }

          conditionalNode = conditionalNode.getNextLink
        }

        if (conditionalPatternBase.isEmpty) {

        }
        else {
          // Build and mine the conditional StreamingFPTree
          val conditionalTree = new StreamingFPTree()
          conditionalTree.insertConditionalFrequentItems(conditionalPatternBase.toList, supportCountRequired)
          conditionalTree.insertConditionalFrequentPatterns(conditionalPatternBase.toList)
          val conditionalFrequentItemsets = conditionalTree.mineItemsets(supportCountRequired)

          if (conditionalFrequentItemsets.nonEmpty)
            for (is <- conditionalFrequentItemsets) {
              is.getItems += headerKey
            }

          branchingItemsets ++= conditionalFrequentItemsets
        }
      }
    }

    if (singlePathItemsets.isEmpty) {
      return branchingItemsets.toList
    }

    // Take the cross product of the mined itemsets
    val ret = ListBuffer[ItemsetWithCount]()

    ret ++= singlePathItemsets
    ret ++= branchingItemsets

    for (i <- singlePathItemsets) {
      for (j <- branchingItemsets) {
        val combinedItem = mutable.HashSet[Int]()
        combinedItem ++= i.getItems
        combinedItem ++= j.getItems
      }
    }
    ret.toList
  }

  def sortByNewOrder(): Unit = {
    // We need to walk the tree from each leaf to each root

  }
}

