package utils.itemset.FPTree

import scala.util.control.Breaks.{break, breakable}
import java.util
/**
 * A class representing a node of a streaming FPTree.
 * @param item the item associated with this node.
 * @param parent the node's parent node.
 * @param count count of occurrences of the Node.
 * @param treeOfOrigin the StreamingFPTree the current Node belongs to
 */
class FPTreeNode(item: Int,
                 var parent: FPTreeNode,
                 var count: Double,
                 treeOfOrigin: StreamingFPTree) extends Serializable {
  private var nextLink: FPTreeNode = _  // Points to the next node with the same item
  private var prevLink: FPTreeNode = _  // Points to the previous node with the same item
  private var children: util.List[FPTreeNode] = _  // Represents the list of child nodes

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
    count = count + by
  }

  /**
   * Method to decrease the count of the item.
   * @param by the count's decrement.
   */
  def decrementCount(by: Double): Unit = {
    count = count - by
  }

  /**
   * Method to check if the node has any children.
   * @return true if node has children, otherwise false.
   */
  def hasChildren: Boolean = {
    children != null && children.size() > 0
  }

  /**
   * Method that removes a child from the List of children.
   * @param child the child for removal.
   */
  def removeChild(child: FPTreeNode): Unit = {
    assert(children.contains(child))
    children.remove(child)
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
  def getChildren: util.List[FPTreeNode] = {
    this.children
  }

  /**
   * Method that merges the children of the current node with another list of children.
   * @param otherChildren a list of additional FPTreeNodes
   */
  def mergeChildren(otherChildren: util.List[FPTreeNode]): Unit = {
    assert(!hasChildren || !treeOfOrigin.leafNodes.contains(this))

    if (otherChildren == null) {
      return
    }

    if (children == null) {
      children = new util.ArrayList[FPTreeNode](otherChildren)
      for (i <- 0 until otherChildren.size()) {
        val child: FPTreeNode = otherChildren.get(i)
        child.parent = this
      }
      treeOfOrigin.leafNodes.remove(this)
      return
    }

    // Create a map to quickly find children by their item
    val ourChildrenMap = new util.HashMap[Int, FPTreeNode]()
    for (i <- 0 until children.size()) {
      val ourChild: FPTreeNode = children.get(i)
      ourChildrenMap.put(ourChild.getItem, ourChild)
    }

    for (i <- 0 until otherChildren.size()) {
      val otherChild: FPTreeNode = otherChildren.get(i)
      otherChild.parent = this
      val ourChild: FPTreeNode = ourChildrenMap.get(otherChild.getItem)

      if (ourChild != null) {
        treeOfOrigin.removeNodeFromHeaders(otherChild)
        ourChild.count += otherChild.count
        ourChild.mergeChildren(otherChild.getChildren)
      } else {
        children.add(otherChild)
        ourChildrenMap.put(otherChild.getItem, otherChild)
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

  def insertTransaction(fullTransaction: util.List[Int],
                        itemCount: Double,
                        currentIndex: Int,
                        streaming: Boolean): Unit = {
    if (!streaming)
      {
        treeOfOrigin.sortedNodes.add(this)
      }

    incrementCount(itemCount)

    if (currentIndex == fullTransaction.size)
      {
        return
      }

    val currentItem: Int = fullTransaction.get(currentIndex)

    var matchingChild: FPTreeNode = null

    if (children != null)
      {
        breakable {
          for (i <- 0 until children.size())
            {
              val child: FPTreeNode = children.get(i)
              if (child.getItem == currentItem)
                {
                  matchingChild = child
                  break()
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

        val prevHeader: FPTreeNode = treeOfOrigin.nodeHeaders.get(currentItem)
        treeOfOrigin.nodeHeaders.put(currentItem, matchingChild)

        if (prevHeader != null)
          {
            matchingChild.setNextLink(prevHeader)
            prevHeader.setPrevLink(matchingChild)
          }

        if (children == null)
          {
            children = new util.ArrayList[FPTreeNode]()
          }

        children.add(matchingChild)

        if (currentIndex == (fullTransaction.size - 1))
          {
            treeOfOrigin.leafNodes.add(matchingChild)
          }

        treeOfOrigin.leafNodes.remove(this)
      }

    matchingChild.insertTransaction(fullTransaction, itemCount, currentIndex + 1, streaming)
  }
}
