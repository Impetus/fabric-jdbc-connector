package com.impetus.fabric.spark.connector.rdd.partitioner
import com.impetus.blkch.spark.connector.BlkchnConnector
import com.impetus.blkch.spark.connector.rdd.ReadConf
import com.impetus.blkch.sql.query.RangeNode
import com.impetus.blkch.util.{Range => BlkchRange}
import java.{lang => jl}
import scala.collection.mutable.ArrayBuffer
import com.impetus.blkch.spark.connector.rdd.partitioner.BlkchnPartition
import com.impetus.blkch.spark.connector.rdd.partitioner.BlkchnPartitioner
import com.impetus.blkch.util.{Range => BlkchRange}
import java.{lang => jl}

class DefaultFabricPartitioner extends BlkchnPartitioner {

  override def getPartitions(connector: BlkchnConnector, readConf: ReadConf): Array[BlkchnPartition] = {
    val rowCount = connector.withStatementDo {
      stat =>
        val rs = stat.executeQuery("SELECT count(block_no) AS cnt FROM block WHERE block_no >= 1")
        rs.next() match {
          case true => rs.getLong("cnt")
          case false => 0l
        }
    }
    var buffer = ArrayBuffer[BlkchnPartition]()
    var start = 1l
    readConf.splitCount match {
      case Some(split) => val partitionRowCount = rowCount / split
                          for(i <- 0 until split) {
                            val rangeNode = new RangeNode[jl.Long]("block", "block_no")
                            rangeNode.getRangeList.addRange(new BlkchRange[jl.Long](start, start + partitionRowCount))
                            buffer = buffer :+ new BlkchnPartition(i, rangeNode, readConf)
                            start = start + partitionRowCount + 1
                          }

      case None =>
        readConf.fetchSizeInRows match {
          case Some(rowSize) => val split = (rowCount / rowSize).toInt
                                for(i <- 0 until split) {
                                  val rangeNode = new RangeNode[jl.Long]("block", "block_no")
                                  rangeNode.getRangeList.addRange(new BlkchRange[jl.Long](start, start + rowSize))
                                  buffer = buffer :+ new BlkchnPartition(i.toInt, rangeNode, readConf)
                                  start = start + rowSize + 1
                                }
          case None => val rangeNode = new RangeNode[jl.Long]("block", "block_no")
                       rangeNode.getRangeList.addRange(new BlkchRange[jl.Long](start, rowCount))
                       buffer = buffer :+ new BlkchnPartition(0, rangeNode, readConf)
        }

    }
    buffer.toArray
  }
}

case object DefaultFabricPartitioner extends DefaultFabricPartitioner