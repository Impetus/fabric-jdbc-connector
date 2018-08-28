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

import org.apache.spark.SparkConf
import org.scalatest.FlatSpec

import com.impetus.test.catagory.UnitTest

@UnitTest
class FabricConnectorConfTest extends FlatSpec {
  
  "FabricConnectorConf" should "read values from Map" in {
    val fcc = FabricConnectorConf(new SparkConf, Map("spark.fabric.configpath" -> "/home/testuser/config",
        "spark.fabric.channel" -> "newchannel", "spark.fabric.username" -> "test",
        "spark.fabric.password" -> "testpw"))
        assert(fcc.configPath.equals("/home/testuser/config"))
        assert(fcc.channel.equals("newchannel"))
        assert(fcc.username.equals("test"))
        assert(fcc.password.equals("testpw"))
  }
  
  it should "take default values in case values are not passed" in {
    val fcc = FabricConnectorConf(new SparkConf)
    assert(fcc.configPath.equals("/tmp/blkchn"))
    assert(fcc.channel.equals("mychannel"))
    assert(fcc.username.equals("impadmin"))
    assert(fcc.password.equals("impadminpw"))
  }
  
  it should "read values from spark conf" in {
    val conf = new SparkConf().set("spark.fabric.configpath", "/home/testuser/config")
                .set("spark.fabric.channel", "newchannel")
                .set("spark.fabric.username", "test")
                .set("spark.fabric.password", "testpw")
    val fcc = FabricConnectorConf(conf)
    assert(fcc.configPath.equals("/home/testuser/config"))
    assert(fcc.channel.equals("newchannel"))
    assert(fcc.username.equals("test"))
    assert(fcc.password.equals("testpw"))
  }
  
  it should "read values from mixture of spark conf and map" in {
    val conf = new SparkConf().set("spark.fabric.configpath", "/home/testuser/config")
                .set("spark.fabric.channel", "newchannel")
    val fcc = FabricConnectorConf(conf, Map("spark.fabric.username" -> "test",
        "spark.fabric.password" -> "testpw"))
        assert(fcc.configPath.equals("/home/testuser/config"))
    assert(fcc.channel.equals("newchannel"))
    assert(fcc.username.equals("test"))
    assert(fcc.password.equals("testpw"))
  }
  
}