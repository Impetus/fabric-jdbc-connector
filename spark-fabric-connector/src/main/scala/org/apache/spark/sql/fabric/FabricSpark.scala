package org.apache.spark.sql.fabric

import javassist.bytecode.stackmap.TypeTag

import com.impetus.blkch.spark.connector.BlkchnConnector
import com.impetus.blkch.spark.connector.rdd.{BlkchnRDD, ReadConf}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

import scala.reflect.ClassTag
import com.impetus.blkch.spark.connector.rdd.partitioner.BlkchnPartitioner
import com.impetus.fabric.spark.connector.rdd.partitioner.DefaultFabricPartitioner

case class FabricSpark(sparkSession: SparkSession, connector: BlkchnConnector, readConf: ReadConf) {

  private def rdd[D: ClassTag]: BlkchnRDD[D] = {
    new BlkchnRDD[D](sparkSession.sparkContext, sparkSession.sparkContext.broadcast(connector), readConf)
  }

  def toRDD[D: ClassTag]: BlkchnRDD[D] = rdd[D]
  
  def toDF(): DataFrame = {
    sparkSession.read.format(FabricFormat)
          .options(readConf.asOptions()).load()
  }

}

object FabricSpark {

  def builder(): Builder = new Builder

  def load[D: ClassTag](sc: SparkContext): BlkchnRDD[D] = load(sc, ReadConf(sc.conf))

  def load[D: ClassTag](sc: SparkContext, readConf: ReadConf): BlkchnRDD[D] = load(sc, readConf, Map())

  def load[D: ClassTag](sc: SparkContext, readConf: ReadConf, options: Map[String, String]): BlkchnRDD[D] = {
    builder().sc(sc).readConf(readConf).options(options).build().toRDD
  }
  
  def load(spark: SparkSession): DataFrame = builder().sparkSession(spark).build().toDF()

  class Builder {

    private var sparkSession: Option[SparkSession] = None
    private var connector: Option[BlkchnConnector] = None
    private var readConfig: Option[ReadConf] = None
    private var options: Map[String, String] = Map()

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

    def option(key: String, value: String): Builder = {
      this.options = this.options + (key -> value)
      this
    }

    def options(options: Map[String, String]): Builder = {
      this.options = options
      this
    }

    def build(): FabricSpark = {
      require(sparkSession.isDefined, "The SparkSession must be set, either explicitly or via the SparkContext")
      val session = sparkSession.get
      val readConf = readConfig match {
        case Some(config) => config
        case None => ReadConf(session.sparkContext.conf, options)
      }
      val conn = connector match {
        case Some(connect) => connect
        case None => new BlkchnConnector(FabricConnectorConf(session.sparkContext.conf, options))
      }
      new FabricSpark(session, conn, readConf)
    }
  }
  
  object implicits extends Serializable {
    implicit def getFabricPartitioner: BlkchnPartitioner = DefaultFabricPartitioner
  }
}
