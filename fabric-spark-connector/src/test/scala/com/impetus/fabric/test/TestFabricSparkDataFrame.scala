package com.impetus.fabric.test

import org.scalatest.FlatSpec
import com.impetus.test.catagory.IntTest
import java.io.File
import java.sql.DriverManager
import com.impetus.blkch.spark.connector.rdd.ReadConf
import org.apache.spark.sql.fabric.FabricSpark
import scala.collection.JavaConverters._
import FabricSpark.implicits._
import java.util.concurrent.TimeUnit
import java.{io => jio}

@IntTest
class TestFabricSparkDataFrame extends FlatSpec with SharedSparkSession {
  
  override def beforeAll() {
    
    def displayProcess(p: Process) {
    var in: jio.BufferedReader = null
      try {
        in = new jio.BufferedReader(new jio.InputStreamReader(p.getInputStream()))
        var str = in.readLine
        while(str != null) {
          println(str);
          str = in.readLine
        }
      } catch {
        case e: Throwable => //IGNORE
      } finally {
        if(in != null) {
          try {
            in.close
          } catch {
            case e: Throwable => //Ignore
          }
        }
      }
    }
    
    println("Staring docker...")
    var process = Runtime.getRuntime.exec("docker-compose -f src/test/resources/basic-network/artifacts/docker-compose.yml up -d --force-recreate")
    process.waitFor
    println("Setting up mysql...")
    process = Runtime.getRuntime.exec("docker exec mysql_db db_scripts/scripts.sh")
    process.waitFor
    println("Setting up fabric...")
    process = Runtime.getRuntime.exec("docker exec -t cli script/script.sh mychannel")
    displayProcess(process)
    process.waitFor
    super.beforeAll()
    
  }
  
  
  
  "FabricSpark" should "read blocks" in {
    Class.forName("com.impetus.fabric.jdbc.FabricDriver")
    val configPath = new File("src/test/resources/basic-network").getAbsolutePath
    val readConf = ReadConf(Some(4), None, "Select * from block where block_no >= 1 and block_no <= 10")
    val df = FabricSpark.load(spark, readConf, Map("spark.fabric.configpath" -> configPath, "spark.fabric.username" -> "admin", "spark.fabric.password" -> "adminpw"))
    assert(10 == df.count)
  }
  
  override def afterAll() {
    val process = Runtime.getRuntime.exec("docker-compose -f src/test/resources/basic-network/artifacts/docker-compose.yml down -v")
    process.waitFor
    super.afterAll()
  }
}