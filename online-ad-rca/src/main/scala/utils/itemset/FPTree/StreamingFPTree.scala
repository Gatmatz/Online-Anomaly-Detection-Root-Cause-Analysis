package utils.itemset.FPTree

import models.ItemsetWithCount

import scala.util.control.Breaks.{break, breakable}
import java.util
import java.util.stream.Collectors
import scala.collection.JavaConverters._


class StreamingFPTree extends Serializable {
  // Create the root
  val root: FPTreeNode = new FPTreeNode(-1, null, 0, this)

  var frequentItemCounts: util.Map[Int, Double] = new util.HashMap[Int, Double]()

  // Used to calculate the order
  val frequentItemOrder: util.Map[Int, Int] = new util.HashMap[Int, Int]()

  var nodeHeaders: util.Map[Int, FPTreeNode] = new util.HashMap[Int, FPTreeNode]()

  var leafNodes: util.Set[FPTreeNode] = new util.HashSet[FPTreeNode]()

  var sortedNodes: util.Set[FPTreeNode] = new util.HashSet[FPTreeNode]()


  /** *****************************************************
   * TREE DEBUGGING FUNCTIONS
   * ******************************************************/

  /**
   * Debug function for Frequent Item Counts
   */
  private def debugFrequentItemCounts(): Unit = {
    println("Frequent Item Counts:")
    frequentItemCounts.entrySet().forEach { e =>
      println(s"${e.getKey} ${e.getValue}")
    }
  }

  /**
   * Debug function for Frequent Item Order
   */
  private def debugFrequentItemOrder(): Unit = {
    println("Frequent Item Order:")
    frequentItemOrder.entrySet().forEach { e =>
      println(s"${e.getKey} ${e.getValue}")
    }
  }

  /**
   * General debug function that prints out the frequent item counts with their order and
   * then proceeds to print out the tree by walking it
   */
  def debugTree(): Unit = {
    debugFrequentItemCounts()
    debugFrequentItemOrder()
    walkTree(root, 1)
  }

  /**
   * Function that traverses a tree starting from a specific node and a specific depth.
   * @param start     the starting point of the traversal
   * @param treeDepth the current tree depth
   */
  private def walkTree(start: FPTreeNode,
                       treeDepth: Int): Unit = {
    val indentation = "\t" * treeDepth
    println("{} node: {}, count: {}, sorted: {}", indentation, start.getItem, start.getCount, sortedNodes.contains(start))

    if (start.getChildren != null) {
      start.getChildren.forEach { child =>
        walkTree(child, treeDepth + 1)
      }
    }
  }

  def decayWeights(start: FPTreeNode,
                   decayWeight: Double): Unit = {
     if (start == root) {
       frequentItemCounts.keySet().forEach { item =>
         frequentItemCounts.put(item, frequentItemCounts.get(item) * decayWeight)
       }
     }

      start.count *= decayWeight
      if (start.getChildren != null) {
        for (i <-0 until start.getChildren.size())
          {
            val child: FPTreeNode = start.getChildren.get(i)
            decayWeights(child, decayWeight)
          }
      }
    }

  def getSupport(pattern: util.Collection[Int]): Int = {
    pattern.forEach{ i =>
      if (!frequentItemCounts.containsKey(i)){
        return 0
      }
    }

    val plist: util.List[Int] = new util.ArrayList[Int](pattern)

    // traverse bottom to top
    plist.sort((i1: Int, i2: Int) => frequentItemOrder.get(i1).compareTo(frequentItemOrder.get(i2)))

    var count: Int = 0
    var pathHead: FPTreeNode = nodeHeaders.get(plist.get(0))
    while (pathHead != null) {
      var curNode: FPTreeNode = pathHead
      var itemsToFind: Int = plist.size()
      breakable {
        while (curNode != null) {
          if (pattern.contains(curNode.getItem))
            itemsToFind -= 1

          if (itemsToFind == 0) {
            count += pathHead.count.toInt
            break()
          }

          curNode = curNode.getParent
        }
      }
      pathHead = pathHead.getNextLink
    }
    count
  }

  def insertFrequentItems(transactions: util.List[util.Set[Int]],
                          countRequiredForSupport: Int): Unit = {
    // Find the count of the items and store them in itemCounts HashMap
    val itemCounts: util.Map[Int, Double] = new util.HashMap[Int, Double]()
    transactions.forEach { t =>
      t.forEach { item =>
        itemCounts.compute(item, (k: Int, v: Double) => if (v == null) 1
        else v + 1)
      }
    }

    // Filter out the item that do no reach the minimum support
    itemCounts.entrySet().forEach { e =>
      if (e.getValue >= countRequiredForSupport)
        frequentItemCounts.put(e.getKey, e.getValue)
    }


    // We have to materialize a canonical order so that items with equal counts
    // are consistently ordered when they are sorted during transaction insertion
    val entrySet = frequentItemCounts.entrySet()
    val sortedItemCounts: util.ArrayList[util.Map.Entry[Int, Double]] = new util.ArrayList[util.Map.Entry[Int, Double]](entrySet)
    sortedItemCounts.sort((entry1: util.Map.Entry[Int, Double], entry2: util.Map.Entry[Int, Double]) => {
      val compareByValue: Int = entry1.getValue.compareTo(entry2.getValue)
      if (compareByValue == 0) {
        // If values are equal, sort by key
        entry1.getKey.compareTo(entry2.getKey)
      }
      else {
        // Otherwise, sort by value
        compareByValue
      }

    })

    for (i <-0 until sortedItemCounts.size())
      {
        frequentItemOrder.put(sortedItemCounts.get(i).getKey, i)
      }
  }

  def deleteItems(itemsToDelete: util.Set[Int]): Unit = {
    if (itemsToDelete == null)
      {
        return
      }

    itemsToDelete.forEach { item =>
      frequentItemOrder.remove(item)

      var nodeToDelete: FPTreeNode = nodeHeaders.get(item)

      while (nodeToDelete != null)
        {
          nodeToDelete.parent.removeChild(nodeToDelete)
          if (nodeToDelete.hasChildren)
            {
              nodeToDelete.parent.mergeChildren(nodeToDelete.getChildren)
            }

          leafNodes.remove(nodeToDelete)

          nodeToDelete = nodeToDelete.getNextLink
        }
    }
  }

  def updateFrequentItemOrder(): Unit = {
    sortedNodes.clear()

    frequentItemOrder.clear()

    // We have to materialize a canonical order so that items with equal counts
    // are consistently ordered when they are sorted during transaction insertion
    val entrySet = frequentItemCounts.entrySet()
    val sortedItemCounts: util.ArrayList[util.Map.Entry[Int, Double]] = new util.ArrayList[util.Map.Entry[Int, Double]](entrySet)
    sortedItemCounts.sort((i1: util.Map.Entry[Int, Double], i2: util.Map.Entry[Int, Double]) => frequentItemCounts.get(i1.getKey).compareTo(frequentItemCounts.get(i2.getKey)))

    // Populate the frequentItemOrder map
    for (i <- 0 until sortedItemCounts.size)
    {
      frequentItemOrder.put(sortedItemCounts.get(i).getKey, i)
    }
  }

  def insertConditionalFrequentItems(patterns:util.List[ItemsetWithCount],
                                     countRequiredForSupport: Int): Unit = {
    val itemCounts: util.Map[Int, Double] = new util.HashMap[Int, Double]()

    patterns.forEach { i =>
      i.getItems.forEach { item =>
        itemCounts.compute(item, (k: Int, v: Double) => if (v == null) i.getCount
        else v + i.getCount)
      }
    }

    itemCounts.entrySet().forEach { e =>
      if (e.getValue >= countRequiredForSupport)
        frequentItemCounts.put(e.getKey, e.getValue)
    }

    updateFrequentItemOrder()
  }

  def sortTransaction(txn: util.List[Int], isStreaming: Boolean): util.List[Int] = {
    if (!isStreaming) {
      txn.sort((i1: Int, i2: Int) => frequentItemOrder.get(i2).compareTo(frequentItemOrder.get(i1)))
    } else {
      txn.sort((i1: Int, i2: Int) => frequentItemOrder.compute(i2, (k: Int, v: Int) => if (v == null) -i2
      else v).compareTo(frequentItemOrder.compute(i1, (k: Int, v: Int) => if (v == null) -i1
      else v)))
    }
    txn
  }

  def reinsertBranch(pattern: util.Set[Int],
                     count: Double,
                     rootOfBranch: FPTreeNode): Unit = {
    val filtered = pattern
      .stream
      .filter((i: Int) => frequentItemCounts.containsKey(i))
      .collect(Collectors.toList[Int])

    sortTransaction(filtered, isStreaming = false)
    rootOfBranch.insertTransaction(filtered, count, 0, streaming = false)
  }

  def insertConditionalFrequentPatterns(patterns: util.List[ItemsetWithCount]): Unit = {
    patterns.forEach { is =>
      reinsertBranch(is.getItems, is.getCount, root)
    }
  }

  def insertTransactions(transactions: util.List[util.Set[Int]],
                         streaming: Boolean,
                         filterExistingFrequentItemsOnly: Boolean): Unit = {
    transactions.forEach { transaction =>
      insertTransaction(transaction, streaming, filterExistingFrequentItemsOnly)
    }
  }

  def insertTransaction(transaction: util.Set[Int],
                        streaming: Boolean,
                        filterExistingFrequentItemsOnly: Boolean): Unit = {
    if (streaming && !filterExistingFrequentItemsOnly)
      {
        transaction.forEach { item =>
          frequentItemCounts.compute(item, (k: Int, v: Double) => if (v == null) 1
          else v + 1)
        }
      }

    val filtered: util.List[Int] = transaction
      .stream
      .filter((i: Int) => frequentItemCounts.containsKey(i))
      .collect(Collectors.toList[Int])

    if (!filtered.isEmpty)
      {
        if (streaming && filterExistingFrequentItemsOnly)
          {
            filtered.forEach { item =>
              frequentItemCounts.compute(item, (k: Int, v: Double) => if (v == null) 1
              else v + 1)
            }
          }

        sortTransaction(filtered, streaming)
        root.insertTransaction(filtered, 1, 0, streaming)
      }
  }

  def removeNodeFromHeaders(node:FPTreeNode): Unit = {
    leafNodes.remove(node)

    if (node.getPrevLink == null)
      {
        assert(nodeHeaders.get(node.getItem) == node)
        nodeHeaders.put(node.getItem, node.getNextLink)
      }
    else
      {
        node.getPrevLink.setNextLink(node.getNextLink)
      }

    if (node.getNextLink != null)
      node.getNextLink.setPrevLink(node.getPrevLink)
  }

  def mineItemsets(supportCountRequired: Int): util.List[ItemsetWithCount] = {
    val singlePathItemsets: util.List[ItemsetWithCount] = new util.ArrayList[ItemsetWithCount]()
    val branchingItemsets: util.List[ItemsetWithCount] = new util.ArrayList[ItemsetWithCount]()

    // Mine single-path itemsets first
    var curNode: FPTreeNode = root
    var nodeOfBranching: FPTreeNode = null
    val singlePathNodes: util.Set[FPTreeNode] = new util.HashSet[FPTreeNode]()

    breakable
    {
      while(true)
        {
          if (curNode.count < supportCountRequired)
              break()

          if (curNode.getChildren != null && curNode.getChildren.size > 1)
            {
              nodeOfBranching = curNode
              break()
            }

          if (curNode != root)
              singlePathNodes.add(curNode)

          if (curNode.getChildren == null || curNode.getChildren.size == 0)
            break()
          else
            curNode = curNode.getChildren.get(0)
        }
    }
    val singlePathNodesScala: Set[FPTreeNode] = singlePathNodes.asScala.toSet
    for (subset <- singlePathNodesScala.subsets()) {
      breakable {
        if (subset.isEmpty)
          break

        var minSupportInSubset: Double = -1
        val items: util.Set[Int] = new util.HashSet[Int]()
        subset.foreach { n =>
          items.add(n.getItem)

          if (minSupportInSubset == -1 || n.getCount < minSupportInSubset)
            minSupportInSubset = n.getCount
        }

        assert(minSupportInSubset >= supportCountRequired)

        singlePathItemsets.add(new ItemsetWithCount(items, minSupportInSubset))
      }
    }

    // the entire tree was a single path
    if (nodeOfBranching == null)
      return singlePathItemsets

    // all of the items in the single path will have been mined now
    // due to the descending frequency count of the StreamingFPTree structure, so
    // we remove them from consideration in the rest
    // instead of destructively removing the nodes from NodeHeader table
    // which would be valid but would make mining non-idempotent, we
    // instead store the nodes to skip in a separate set
    val alreadyMinedItems = new util.HashSet[Int]()
    singlePathNodes.forEach { node =>
      alreadyMinedItems.add(node.getItem)
    }

    nodeHeaders.entrySet().forEach { header =>
      breakable {
        if (alreadyMinedItems.contains(header.getKey) || (frequentItemCounts.get(header.getKey) < supportCountRequired))
          break()

        // add the singleton item set
        branchingItemsets.add(new ItemsetWithCount(new util.HashSet[Int](header.getKey),frequentItemCounts.get(header.getKey)))

        val conditionalPatternBase: util.List[ItemsetWithCount] = new util.ArrayList[ItemsetWithCount]()

        // walk each "leaf" node
        var conditionalNode: FPTreeNode = header.getValue
        while (conditionalNode != null)
          {
            val leafSupport: Double = conditionalNode.getCount

            // walk the tree up to the branch node
            val conditionalPattern: util.Set[Int] = new util.HashSet[Int]()
            var walkNode: FPTreeNode = conditionalNode.getParent
            while (walkNode != nodeOfBranching.getParent && walkNode != root)
              {
                conditionalPattern.add(walkNode.getItem)
                walkNode = walkNode.getParent
              }

            if (conditionalPattern.size() > 0)
              conditionalPatternBase.add(new ItemsetWithCount(conditionalPattern, leafSupport))

            conditionalNode = conditionalNode.getNextLink
          }

        if (conditionalPatternBase.isEmpty)
          break()

        // build and mine the conditional StreamingFPTree
        val conditionalTree: StreamingFPTree = new StreamingFPTree
        conditionalTree.insertConditionalFrequentItems(conditionalPatternBase,supportCountRequired)
        conditionalTree.insertConditionalFrequentPatterns(conditionalPatternBase)
        val conditionalFrequentItemsets: util.List[ItemsetWithCount] = conditionalTree.mineItemsets(supportCountRequired)

        if (!conditionalFrequentItemsets.isEmpty)
          {
            conditionalFrequentItemsets.forEach { is =>
              is.getItems.add(header.getKey)
            }

            branchingItemsets.addAll(conditionalFrequentItemsets)
          }
      }
    }

    if (singlePathItemsets.isEmpty) {
      return branchingItemsets
    }

    // Take the cross product of the mined itemsets
    val ret: util.List[ItemsetWithCount] = new util.ArrayList[ItemsetWithCount]()

    ret.addAll(singlePathItemsets)
    ret.addAll(branchingItemsets)

    singlePathItemsets.forEach { i =>
      branchingItemsets.forEach { j =>
        val combinedItems = new util.HashSet[Int]()
        combinedItems.addAll(i.getItems)
        combinedItems.addAll(j.getItems)
        ret.add(new ItemsetWithCount(combinedItems, Math.min(i.getCount, j.getCount)))
      }
    }
    ret
  }

  def sortByNewOrder(): Unit = {
    // We need to walk the tree from each leaf to each root
    val leavesToInspect = new util.ArrayList[FPTreeNode](leafNodes)
    val removedNodes = new util.HashSet[FPTreeNode]()
    for (i <- 0 until leavesToInspect.size)
      {
        val leaf: FPTreeNode = leavesToInspect.get(i)
        breakable
        {
          if (leaf == root)
            break

          if (removedNodes.contains(leaf) || sortedNodes.contains(leaf))
            break

          val leafCount: Double = leaf.getCount

          val toInsert: util.Set[Int] = new util.HashSet[Int]()

          toInsert.add(leaf.getItem)

          assert(!leaf.hasChildren)

          removeNodeFromHeaders(leaf)

          removedNodes.add(leaf)

          var curLowestNodeOrder: Int = frequentItemOrder.get(leaf.getItem)

          var node: FPTreeNode = leaf.getParent
          node.removeChild(leaf)

          breakable {
            while(true)
              {
                if (node == root)
                  break()

                val nodeOrder: Int = frequentItemOrder.get(node.getItem)
                if (sortedNodes.contains(node) && nodeOrder < curLowestNodeOrder)
                  break()
                else if (nodeOrder < curLowestNodeOrder)
                  {
                    curLowestNodeOrder = nodeOrder
                  }

                assert(!removedNodes.contains(node))

                toInsert.add(node.getItem)

                node.decrementCount(leafCount)

                if (node.getCount == 0 && !node.hasChildren)
                  {
                    removedNodes.add(node)
                    removeNodeFromHeaders(node)
                    node.getParent.removeChild(node)
                  }
                else if (!node.hasChildren && !sortedNodes.contains(node))
                  leavesToInspect.add(node)

                node = node.getParent
              }

          }

          node.decrementCount(leafCount)

          reinsertBranch(toInsert, leafCount, node)
        }
      }
  }
}

