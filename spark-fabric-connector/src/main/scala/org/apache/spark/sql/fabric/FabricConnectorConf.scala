/*******************************************************************************
* * Copyright 2018 Impetus Infotech.
* *
* * Licensed under the Apache License, Version 2.0 (the "License");
* * you may not use this file except in compliance with the License.
* * You may obtain a copy of the License at
* *
* * http://www.apache.org/licenses/LICENSE-2.0
* *
* * Unless required by applicable law or agreed to in writing, software
* * distributed under the License is distributed on an "AS IS" BASIS,
* * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* * See the License for the specific language governing permissions and
* * limitations under the License.
******************************************************************************/
package org.apache.spark.sql.fabric

import java.sql.DriverManager

import com.impetus.blkch.jdbc.BlkchnConnection
import com.impetus.blkch.spark.connector.BlkchnConnectorConf
import com.impetus.blkch.spark.connector.util.ConfigParam
import com.impetus.fabric.jdbc.FabricConnection
import org.apache.spark.SparkConf

class FabricConnectorConf(conf: SparkConf, options: Map[String, String]) extends BlkchnConnectorConf(conf) {

  val configPath = conf.get(FabricConnectorConf.ConfigPath.name,
                    options.getOrElse(FabricConnectorConf.ConfigPath.name, FabricConnectorConf.ConfigPath.default))
  val channel = conf.get(FabricConnectorConf.Channel.name,
                    options.getOrElse(FabricConnectorConf.Channel.name, FabricConnectorConf.Channel.default))
  val username = conf.get(FabricConnectorConf.Username.name,
                    options.getOrElse(FabricConnectorConf.Username.name, FabricConnectorConf.Username.default))
  val password = conf.get(FabricConnectorConf.Password.name,
                    options.getOrElse(FabricConnectorConf.Password.name, FabricConnectorConf.Password.default))

  override def getConnection(): BlkchnConnection = {
    val jdbcUrl = "jdbc:fabric://" + configPath + ":" + channel
    DriverManager.getConnection(jdbcUrl, username, password).asInstanceOf[BlkchnConnection]
  }

  override def toString: String = {
    val sb = new StringBuilder("[")
    sb.append("[" + FabricConnectorConf.ConfigPath.name + ":" + configPath + "]")
    sb.append("[" + FabricConnectorConf.Channel.name + ":" + channel + "]")
    sb.append("[" + FabricConnectorConf.Username.name + ":" + username + "]")
    sb.append("[" + FabricConnectorConf.Password.name + ":" + password + "]")
    sb.toString()
  }
}

object FabricConnectorConf {

  val ConfigPath = ConfigParam[String]("spark.fabric.configpath",
    "/tmp/blkchn",
    """
      |Hyperledger fabric abric onfiguration directory's path. This folder contains all the required certificates, connection details etc.
    """.stripMargin)

  val Channel = ConfigParam[String]("spark.fabric.channel",
    "mychannel",
    """
      | Name of hyperledger fabric channel to connect this connector with.
    """.stripMargin)

  val Username = ConfigParam[String]("spark.fabric.username",
    "impadmin",
    """
      | Username of hyperledger fabric network from which connection should be made.
    """.stripMargin)

  val Password = ConfigParam[String]("spark.fabric.password",
    "impadminpw",
    """
      | Password of hyperledger fabric network user from which connection should be made.
    """.stripMargin)

  def apply(conf: SparkConf): FabricConnectorConf = new FabricConnectorConf(conf, Map())

  def apply(conf: SparkConf, options: Map[String, String]): FabricConnectorConf = new FabricConnectorConf(conf, options)
}
