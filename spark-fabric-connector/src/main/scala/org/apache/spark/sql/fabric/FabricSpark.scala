package org.apache.spark.sql.fabric

import javassist.bytecode.stackmap.TypeTag

import com.impetus.blkch.spark.connector.BlkchnConnector
import com.impetus.blkch.spark.connector.rdd.ReadConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

import scala.reflect.ClassTag
import com.impetus.blkch.spark.connector.rdd.partitioner.BlkchnPartitioner
import com.impetus.fabric.spark.connector.rdd.partitioner.DefaultFabricPartitioner
//import com.impetus.fabric.spark.connector.rdd.FabricRDD
import com.impetus.blkch.spark.connector.rdd.BlkchnRDD
import com.impetus.fabric.spark.connector.rdd.WriteConf
import org.apache.spark.sql.Row

case class FabricSpark private(sparkSession: SparkSession, connector: BlkchnConnector, readConf: ReadConf, 
    writeConf: WriteConf, options: Map[String, String]) {

  private def rdd[D: ClassTag]: BlkchnRDD[D] = {
    new BlkchnRDD[D](sparkSession.sparkContext, sparkSession.sparkContext.broadcast(connector), readConf)
  }

  def toRDD[D: ClassTag]: BlkchnRDD[D] = rdd[D]
  
  def toDF(): DataFrame = {
    val readConfOptions = readConf.asOptions
    val extraOptions = for((key, value) <- options ; if(!readConfOptions.contains(key))) yield {
      (key, value)
    }
    sparkSession.read.format(FabricFormat)
          .options(readConfOptions).options(extraOptions).load()
  }
  
  def save(dataframe: DataFrame): Unit = {
    
    def createInsertStat(row: Row): String = {
      val sb = new StringBuilder
              sb.append("INSERT INTO ")
              sb.append(writeConf.chaincode)
              sb.append(" VALUES(")
              sb.append("'" + writeConf.function + "'")
              for(i <- 0 until row.size) {
                sb.append(",")
                sb.append("'" + (if(row.get(i) == null) null else row.get(i).toString) + "'")
              }
              sb.append(")")
              sb.toString
    }
    
    dataframe.foreachPartition { 
      rows =>
        connector.withStatementDo { 
          stat => 
            for(row <- rows) {
              val insertCmd = createInsertStat(row)
              stat.execute(insertCmd)
            } 
        }
    }
  }

}

object FabricSpark {

  private def builder(): Builder = new Builder

  def load[D: ClassTag](sc: SparkContext): BlkchnRDD[D] = load(sc, ReadConf(sc.conf))

  def load[D: ClassTag](sc: SparkContext, readConf: ReadConf): BlkchnRDD[D] = load(sc, readConf, Map())

  def load[D: ClassTag](sc: SparkContext, readConf: ReadConf, options: Map[String, String]): BlkchnRDD[D] = {
    builder().sc(sc).readConf(readConf).options(options).build().toRDD
  }
  
  def load(spark: SparkSession): DataFrame = load(spark, ReadConf(spark.sparkContext.conf))
  
  def load(spark: SparkSession, readConf: ReadConf): DataFrame = load(spark, readConf, Map())
  
  def load(spark: SparkSession, readConf: ReadConf, options: Map[String, String]): DataFrame = {
    builder().sparkSession(spark).readConf(readConf).options(options).build().toDF()
  }
  
  def save(dataframe: DataFrame): Unit = save(dataframe, WriteConf(dataframe.sparkSession.sparkContext.conf))
  
  def save(dataframe: DataFrame, writeConf: WriteConf): Unit = save(dataframe, writeConf, Map())
  
  def save(dataframe: DataFrame, writeConf: WriteConf, options: Map[String, String]): Unit = {
    builder().sparkSession(dataframe.sparkSession).writeConf(writeConf).options(options).
                        mode(Mode.Write).build().save(dataframe)
  }

  private class Builder {

    private var sparkSession: Option[SparkSession] = None
    private var connector: Option[BlkchnConnector] = None
    private var readConfig: Option[ReadConf] = None
    private var writeConfig: Option[WriteConf] = None
    private var options: Map[String, String] = Map()
    private var mode: Mode.Value = Mode.Read

    def sparkSession(sparkSession: SparkSession): Builder = {
      this.sparkSession = Some(sparkSession)
      this
    }

    def sc(sc: SparkContext): Builder = {
      this.sparkSession = Some(SparkSession.builder().config(sc.getConf).getOrCreate())
      this
    }

    def connector(connector: BlkchnConnector): Builder = {
      this.connector = Some(connector)
      this
    }

    def readConf(readConf: ReadConf): Builder = {
      this.readConfig = Some(readConf)
      this
    }
    
    def writeConf(writeConf: WriteConf): Builder = {
      this.writeConfig = Some(writeConf)
      this
    }

    def option(key: String, value: String): Builder = {
      this.options = this.options + (key -> value)
      this
    }

    def options(options: Map[String, String]): Builder = {
      this.options = options
      this
    }
    
    def mode(mode: Mode.Value): Builder = {
      this.mode = mode
      this
    }

    def build(): FabricSpark = {
      require(sparkSession.isDefined, "The SparkSession must be set, either explicitly or via the SparkContext")
      val session = sparkSession.get
      val readConf = readConfig match {
        case Some(config) => config
        case None => mode match {
          case Mode.Read => ReadConf(session.sparkContext.conf, options)
          case Mode.Write => null
        }
      }
      val writeConf = writeConfig match {
        case Some(config) => config
        case None => mode match {
          case Mode.Read => null
          case Mode.Write => WriteConf(session.sparkContext.conf, options)
        }
      }
      val conn = connector match {
        case Some(connect) => connect
        case None => new BlkchnConnector(FabricConnectorConf(session.sparkContext.conf, options))
      }
      new FabricSpark(session, conn, readConf, writeConf, options)
    }
  }
  
  private object Mode extends Enumeration {
    type Mode = Value
    
    val Read = Value
    val Write = Value
  }
  
  object implicits extends Serializable {
    implicit def getFabricPartitioner: BlkchnPartitioner = DefaultFabricPartitioner
  }
}
