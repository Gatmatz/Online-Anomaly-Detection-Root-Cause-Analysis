package utils.itemset.FPTree

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}

/**
 * A class representing a node of a streaming FPTree.
 * @param item the item associated with this node.
 * @param parent the node's parent node.
 * @param count count of occurrences of the Node.
 * @param treeOfOrigin the StreamingFPTree the current Node belongs to
 */
class FPTreeNode(item:Int, var parent: FPTreeNode, var count: Double, treeOfOrigin: StreamingFPTree) {
  private var nextLink: FPTreeNode = _  // Points to the next node with the same item
  private var prevLink: FPTreeNode = _  // Points to the previous node with the same item
  private var children: ListBuffer[FPTreeNode] = _  // Represents the list of child nodes

  /**
   * Getter function for the node's item.
   * @return node's item.
   */
  def getItem: Int = {
    item
  }

  /**
   * Getter function for the item's count.
   * @return item's count.
   */
  def getCount: Double = {
    count
  }

  /**
   * Method to increase the count of the item.
   * @param by the count's increment.
   */
  def incrementCount(by: Double): Unit = {
    count += by
  }

  /**
   * Method to decrease the count of the item.
   * @param by the count's decrement.
   */
  def decrementCount(by: Double): Unit = {
    count -= by
  }

  /**
   * Method to check if the node has any children.
   * @return true if node has children, otherwise false.
   */
  def hasChildren: Boolean = {
    children != null && children.nonEmpty
  }

  /**
   * Method that removes a child from the List of children.
   * @param child the child for removal.
   */
  def removeChild(child: FPTreeNode): Unit = {
    assert(children.contains(child))
    children = children.filterNot(_ == child)
  }

  /**
   * Setter for the next Link of the specific Node.
   * @param nextLink the FPTreeNode that represents the next Node.
   */
  def setNextLink(nextLink: FPTreeNode): Unit = {
    this.nextLink = nextLink
  }

  /**
   * Getter for the FPTreeNode that represents the next link of current Node.
   * @return a FPTreeNode that comes after the current Node.
   */
  def getNextLink: FPTreeNode = {
    this.nextLink
  }

  /**
   * Setter for the previous Link of the specific Node.
   * @param prevLink the FPTreeNode that represents the previous Node.
   */
  def setPrevLink(prevLink: FPTreeNode): Unit = {
    this.prevLink = prevLink
  }

  /**
   * Getter for the FPTreeNode that represents the previous link of current Node.
   * @return a FPTreeNode that comes before the current Node.
   */
  def getPrevLink: FPTreeNode = {
    this.prevLink
  }

  /**
   * Method that retrieves the parent node.
   * @return a parent FPTreeNode.
   */
  def getParent: FPTreeNode = {
    this.parent
  }

  /**
   * Method that retrieves the children of the Node.
   * @return a list of FPTreeNodes that are children of the current node.
   */
  def getChildren: ListBuffer[FPTreeNode] = {
    this.children
  }

  /**
   * Method that merges the children of the current node with another list of children.
   * @param otherChildren a list of additional FPTreeNodes
   */
  def mergeChildren(otherChildren: ListBuffer[FPTreeNode]): Unit = {
    assert(!hasChildren || !treeOfOrigin.leafNodes.contains(this))

    if (otherChildren == null)
    {
      return
    }

    if (children == null)
      {
        children = otherChildren.map { child =>
          child.parent = this
          child
        }
        treeOfOrigin.leafNodes -= this
        return
      }

    // O(N^2); slow for large lists; consider optimizing
    for (otherChild <- otherChildren)
    {
      otherChild.parent = this
      var matched: Boolean = false
      breakable { // Use breakable to be able to break out of the loop
        for (ourChild <- children) {
          if (otherChild.getItem == ourChild.getItem) {
            treeOfOrigin.removeNodeFromHeaders(otherChild)
            ourChild.incrementCount(otherChild.getCount)
            ourChild.mergeChildren(otherChild.getChildren)
            matched = true
            break  // Break out of the loop
          }
        }
      }

      if (!matched)
        {
          this.children ++= otherChildren
        }
    }
  }

  /**
   * Method that inserts the transaction at this node starting with transaction[currentIndex]
   * and then find the child that matches.
   * Inserts a transaction (sequence of items) into the tree starting from this node.
   * @param fullTransaction the transaction to be added to the tree
   * @param itemCount the count of the transaction
   * @param currentIndex the index of the transaction
   */

  def insertTransaction(fullTransaction: List[Int], itemCount: Double ,currentIndex: Int, streaming: Boolean): Unit = {
    if (!streaming)
      {
        treeOfOrigin.sortedNodes.add(this)
      }

    incrementCount(itemCount)

    if (currentIndex == fullTransaction.size)
      {
        return
      }

    val currentItem: Int = fullTransaction(currentIndex)

    var matchingChild: FPTreeNode = null

    if (children != null)
      {
        breakable {
          for (child <- children) {
            if (child.getItem == currentItem) {
              matchingChild = child
              break
            }
          }
        }
      }

    if (matchingChild == null)
      {
        matchingChild =  new FPTreeNode(currentItem, this, 0, treeOfOrigin)

        if (!streaming)
          {
            treeOfOrigin.sortedNodes.add(matchingChild)
          }

        val prevHeader: FPTreeNode = treeOfOrigin.nodeHeaders.getOrElse(currentItem, null)
        treeOfOrigin.nodeHeaders.put(currentItem, matchingChild)

        if (prevHeader != null)
          {
            matchingChild.setNextLink(prevHeader)
            prevHeader.setPrevLink(matchingChild)
          }

        if (children == null)
          {
            children = ListBuffer.empty[FPTreeNode]
          }

        children += matchingChild

        if (currentIndex == (fullTransaction.size - 1))
          {
            treeOfOrigin.leafNodes += matchingChild
          }

        treeOfOrigin.leafNodes -= this
      }

    matchingChild.insertTransaction(fullTransaction, itemCount, currentIndex + 1, streaming)
  }
}
