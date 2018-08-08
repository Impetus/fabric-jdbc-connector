package com.impetus.fabric.spark.connector.rdd.partitioner
import com.impetus.blkch.spark.connector.BlkchnConnector
import com.impetus.blkch.spark.connector.rdd.ReadConf
import com.impetus.blkch.sql.query.RangeNode
import com.impetus.blkch.util.{Range => BlkchRange}
import java.{lang => jl}
import scala.collection.mutable.ArrayBuffer
import com.impetus.blkch.spark.connector.rdd.partitioner.BlkchnPartition
import com.impetus.blkch.spark.connector.rdd.partitioner.BlkchnPartitioner
import scala.collection.JavaConversions._

class DefaultFabricPartitioner extends BlkchnPartitioner {

  override def getPartitions(connector: BlkchnConnector, readConf: ReadConf): Array[BlkchnPartition] = {
    val (rawRangesNodes, blockHeight) = connector.withStatementDo {
      stat =>
        (stat.getProbableRange(readConf.query), stat.getBlockHeight.asInstanceOf[jl.Long])
    }
    
    val rangesNodes = new RangeNode[jl.Long](rawRangesNodes.getTable, rawRangesNodes.getColumn);
    for(range <- rawRangesNodes.getRangeList.getRanges) {
      val newMin: jl.Long = if(range.getMin.asInstanceOf[jl.Long] == jl.Long.MIN_VALUE)  1l 
                            else range.getMin.asInstanceOf[jl.Long]
      val newMax: jl.Long = if(range.getMax.asInstanceOf[jl.Long] == jl.Long.MAX_VALUE) blockHeight - 1
                            else range.getMax.asInstanceOf[jl.Long]
      rangesNodes.getRangeList.addRange(new BlkchRange(newMin, newMax))
    }
    
    def getRowCountInRange (range: (jl.Long, jl.Long)): jl.Long = (range._2 - range._1) + 1
    
    val rangesTuple = rangesNodes.getRangeList.getRanges.toList.map{
      x =>
        val min: jl.Long =x.getMin.asInstanceOf[jl.Long]
        val max: jl.Long = x.getMax.asInstanceOf[jl.Long]
        (min, max)
    }
    
    val rangeSumList = rangesTuple.map{ x => getRowCountInRange(x._1,x._2)}
    val rangesTotalSum = rangeSumList.reduce(_ + _)
    
    def getRanges(rows: jl.Long,split: jl.Long): Seq[RangeNode[jl.Long]] = {
      var ranges:List[jl.Long] = rangeSumList
      var currRows: List[(jl.Long, jl.Long)] = rangesTuple
      for(i <- 0l until split) yield {
        var currRowsSum = 0l
        var currRowsList = new scala.collection.mutable.ListBuffer[(jl.Long, jl.Long)]
        while (currRowsSum < rows && !currRows.isEmpty) {
          if (ranges.head <= rows - currRowsSum) {
            currRowsSum += ranges.head
            ranges = ranges.tail
            currRowsList = currRowsList :+ (currRows.head._1, currRows.head._2)
            currRows = currRows.tail
          } else if (ranges.head > rows - currRowsSum) {
            currRowsSum += rows
            ranges = (ranges.head - rows) :: ranges.tail
            currRowsList = currRowsList :+ (currRows.head._1, (currRows.head._1 + rows - 1).asInstanceOf[jl.Long])
            currRows = ((currRows.head._1 + rows).asInstanceOf[jl.Long], currRows.head._2) :: currRows.tail
          }
        }
        getRangeNodeFromRanges(currRowsList.toList)
      }
    }
    
    def getRangeNodeFromRanges(rngLst: List[(jl.Long, jl.Long)]) ={
      /*Passing table name null and call setTableName function from physical plan paginate method*/
      val rangeNode = new RangeNode[jl.Long]("","block_no")
      for((x,y) <- rngLst){
        rangeNode.getRangeList.addRange(new BlkchRange[jl.Long](x, y))
      }
      rangeNode
    }
    
    var buffer = ArrayBuffer[BlkchnPartition]()
    val start = 1l
    readConf.splitCount match {
      case Some(split) =>
        require(split > 0, s"Split should be positive : $split")
        val partitionRowCount = if((rangesTotalSum / split) * split < rangesTotalSum) (rangesTotalSum / split) + 1 else rangesTotalSum / split
        val rangeNodes = getRanges(partitionRowCount, split)
        for((rangenode,i) <- rangeNodes.zipWithIndex){
          buffer = buffer :+ new BlkchnPartition(i, rangenode, readConf)
        }

      case None =>
        readConf.fetchSizeInRows match {
          case Some(rowSize) =>
            require(rowSize > 0, s"Row Size should be positive : $rowSize")
            val split = if((rangesTotalSum / rowSize) * rowSize < rangesTotalSum) (rangesTotalSum / rowSize) + 1 else (rangesTotalSum / rowSize)
            val rangeNodes = getRanges(rowSize, split)
            for((rangenode,i) <- rangeNodes.zipWithIndex){
              buffer = buffer :+ new BlkchnPartition(i, rangenode, readConf)
            }
            
          case None =>
            /*Passing table name null and call setTableName function from physical plan paginate method*/
            val rangeNode = new RangeNode[jl.Long]("","block_no")
            rangeNode.getRangeList.addRange(new BlkchRange[jl.Long](start, rangesTotalSum))
            buffer = buffer :+ new BlkchnPartition(0, rangeNode, readConf)
        }
    }
    buffer.toArray
  }
}
  
case object DefaultFabricPartitioner extends DefaultFabricPartitioner