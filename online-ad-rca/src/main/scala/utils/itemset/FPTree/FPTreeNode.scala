package utils.itemset.FPTree

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}

class FPTreeNode(item:Int, var parent: FPTreeNode, var count: Double) {
  private var nextLink: FPTreeNode = _
  private var prevLink: FPTreeNode = _
  private var children: ListBuffer[FPTreeNode] = _

  def getItem: Int = {
    item
  }

  def getCount: Double = {
    count
  }

  def incrementCount(by: Double): Unit = {
    count += by
  }

  def decrementCount(by: Double): Unit = {
    count -= by
  }

  def hasChildren: Boolean = {
    children != null && children.nonEmpty
  }

  def removeChild(child: FPTreeNode): Unit = {
    assert(children.contains(child))
    children = children.filterNot(_ == child)
  }

  def setNextLink(nextLink: FPTreeNode): Unit = {
    this.nextLink = nextLink
  }

  def getNextLink: FPTreeNode = {
    this.nextLink
  }

  def setPrevLink(prevLink: FPTreeNode): Unit = {
    this.prevLink = prevLink
  }

  def getPrevLink: FPTreeNode = {
    this.prevLink
  }

  def getParent: FPTreeNode = {
    this.parent
  }

  def getChildren: ListBuffer[FPTreeNode] = {
    this.children
  }

  def mergeChildren(otherChildren: ListBuffer[FPTreeNode], leafNodes: mutable.HashSet[FPTreeNode]): Unit = {
    assert(!hasChildren || !leafNodes.contains(this))

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
        leafNodes -= this
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
//            removeNodeFromHeaders(otherChild)
            ourChild.incrementCount(otherChild.getCount)
            ourChild.mergeChildren(otherChild.getChildren, leafNodes)
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

  // insert the transaction at this node starting with transaction[currentIndex]
  // then find the child that matches
  def insertTransaction(fullTransaction: ListBuffer[Int], itemCount: Double ,currentIndex: Int, nodeHeaders: mutable.Map[Int, FPTreeNode], leafNodes:mutable.HashSet[FPTreeNode]): Unit = {
    incrementCount(itemCount)

    if (currentIndex == fullTransaction.size)
      {
        return
      }

    var currentItem: Int = fullTransaction(currentIndex)

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
        matchingChild =  new FPTreeNode(currentItem, this, 0)

        var prevHeader: FPTreeNode = nodeHeaders(currentItem)
        nodeHeaders.put(currentItem, matchingChild)

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
            leafNodes += matchingChild
          }

        leafNodes -= this
      }

    matchingChild.insertTransaction(fullTransaction, itemCount, currentIndex + 1 ,nodeHeaders, leafNodes)
  }
}
