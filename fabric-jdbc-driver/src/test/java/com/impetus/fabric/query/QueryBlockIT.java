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

package com.impetus.fabric.query;


import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.TestCase;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.impetus.test.catagory.IntegrationTest;

@Category(IntegrationTest.class)
public class QueryBlockIT extends TestCase {

    @Test
    public void testFabricStatement() throws ClassNotFoundException, SQLException {
        Class.forName("com.impetus.fabric.jdbc.FabricDriver");
        File configFolder = new File("src/test/resources/blockchain-query");
        String configPath = configFolder.getAbsolutePath();
        Connection conn = DriverManager.getConnection("jdbc:fabric://" + configPath+":mychannel", "Impetus User", "");
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("select * from block where block_no = 2");
        assert(rs.next());
    }

    @Test
    public void testSaveAssetToLocalDB() throws ClassNotFoundException, SQLException{

        Class.forName("com.impetus.fabric.jdbc.FabricDriver");
        File configFolder = new File("src/test/resources/blockchain-query");
        String configPath = configFolder.getAbsolutePath();
        Connection conn = DriverManager.getConnection("jdbc:fabric://" + configPath+":mychannel", "Impetus User", "");
        Statement stat = conn.createStatement();

        //Delete Asset if Exists
        String sqlDelete = "Drop ASSET user_asset1";
        stat.execute(sqlDelete);

        String sqlCreate = "CREATE ASSET user_asset1"
                + " WITH STORAGE TYPE JSON "
                + "FIELDS DELIMITED BY ',' "
                + "RECORDS DELIMITED BY \"\\n\"";
        stat.execute(sqlCreate);

    }

    @Test
    public void testDeleteAsset() throws ClassNotFoundException, SQLException{

        Class.forName("com.impetus.fabric.jdbc.FabricDriver");
        File configFolder = new File("src/test/resources/blockchain-query");
        String configPath = configFolder.getAbsolutePath();
        Connection conn = DriverManager.getConnection("jdbc:fabric://" + configPath+":mychannel", "Impetus User", "");
        Statement stat = conn.createStatement();
        String sql = "Drop ASSET user_asset1";
        stat.execute(sql);
    }


    @Test
    public void testSaveAssetWithFields() throws ClassNotFoundException, SQLException{

        Class.forName("com.impetus.fabric.jdbc.FabricDriver");
        File configFolder = new File("src/test/resources/blockchain-query");
        String configPath = configFolder.getAbsolutePath();
        Connection conn = DriverManager.getConnection("jdbc:fabric://" + configPath+":mychannel", "Impetus User", "");
        Statement stat = conn.createStatement();

        //Delete Asset if Exists
        String sqlDelete = "Drop ASSET user_asset1";
        stat.execute(sqlDelete);


        String sqlCreate = "CREATE ASSET user_asset1(a Integer, b String)"
                + " WITH STORAGE TYPE CSV "
                + "FIELDS DELIMITED BY ',' "
                + "RECORDS DELIMITED BY \"\\n\"";
        stat.execute(sqlCreate);


    }



    @Test
    public void testFabricStatementWithOrderByAndGroupBy() throws ClassNotFoundException, SQLException {
        Class.forName("com.impetus.fabric.jdbc.FabricDriver");
        File configFolder = new File("src/test/resources/blockchain-query");
        String configPath = configFolder.getAbsolutePath();
        Connection conn = DriverManager.getConnection("jdbc:fabric://" + configPath+":mychannel", "Impetus User", "");
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("select block_no, sum(block_no) from block where block_no >= 2 and block_no <= 5  group by block_no order by block_no DESC ");
        assert(rs.next());
    }


    @Test
    //this query should return empty dataset.
    public void testANdOROnQuery() throws ClassNotFoundException, SQLException {
        Class.forName("com.impetus.fabric.jdbc.FabricDriver");
        File configFolder = new File("src/test/resources/blockchain-query");
        String configPath = configFolder.getAbsolutePath();
        Connection conn = DriverManager.getConnection("jdbc:fabric://" + configPath+":mychannel", "Impetus User", "");
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("select * from block where (block_no = 2 or block_no = 3) and (block_no =4 or block_no = 5)");
        assert(!rs.next());
    }


    // No need to Assert, test passed if didnt throw exception
    public void testCreateFunction() throws ClassNotFoundException, SQLException{
        long currentTimeStamp = System.currentTimeMillis();
        Class.forName("com.impetus.fabric.jdbc.FabricDriver");
        File configFolder = new File("src/test/resources/blockchain-query");
        String configPath = configFolder.getAbsolutePath();
        Connection conn = DriverManager.getConnection("jdbc:fabric://" + configPath+":mychannel", "Impetus User", "");
        Statement stat = conn.createStatement();
        String createFuncQuery = "CREATE FUNCTION chncodefunc"+currentTimeStamp+" AS 'assettransfer' WITH VERSION '1.0'";
        stat.execute(createFuncQuery);
    }

    // No need to Assert, test passed if didnt throw exception
    public void testCallFunction() throws ClassNotFoundException, SQLException{
        long currentTimeStamp = System.currentTimeMillis();
        Class.forName("com.impetus.fabric.jdbc.FabricDriver");
        File configFolder = new File("src/test/resources/blockchain-query");
        String configPath = configFolder.getAbsolutePath();
        Connection conn = DriverManager.getConnection("jdbc:fabric://" + configPath+":mychannel", "Impetus User", "");
        Statement stat = conn.createStatement();
        String createFuncQuery = "CREATE FUNCTION chncodefunc"+currentTimeStamp+" AS 'assettransfer' WITH VERSION '1.0'";
        System.out.println(stat.execute(createFuncQuery));

        String callQuery = "CALL chncodefunc"+currentTimeStamp+"('getAsset', 1000)";
        stat.execute(callQuery);


    }

    // No need to Assert, test passed if didnt throw exception
    public void testInsertFunction() throws ClassNotFoundException, SQLException{
        long currentTimeStamp = System.currentTimeMillis();
        Class.forName("com.impetus.fabric.jdbc.FabricDriver");
        File configFolder = new File("src/test/resources/blockchain-query");
        String configPath = configFolder.getAbsolutePath();
        Connection conn = DriverManager.getConnection("jdbc:fabric://" + configPath+":mychannel", "Impetus User", "");
        Statement stat = conn.createStatement();

        String createFuncQuery = "CREATE FUNCTION chncodefunc"+currentTimeStamp+" AS 'assettransfer' WITH VERSION '1.0'";
        stat.execute(createFuncQuery);
        String insertQuery = "INSERT INTO chncodefunc"+currentTimeStamp+" VALUES('transferAsset', 1001, 2001, 2002)";
        stat.execute(insertQuery);
    }



    @Test
    public void testQueryWithExpectedEmptyReturn() throws ClassNotFoundException, SQLException {
        Class.forName("com.impetus.fabric.jdbc.FabricDriver");
        File configFolder = new File("src/test/resources/blockchain-query");
        String configPath = configFolder.getAbsolutePath();
        Connection conn = DriverManager.getConnection("jdbc:fabric://" + configPath+":mychannel", "Impetus User", "");
        Statement stat = conn.createStatement();

        ResultSet rs = stat.executeQuery("select * from block blk where block_no >= -2 and block_no <= 1");

        assert(rs.next());
    }
    
    @Test
    public void testUpgradeChaincode() throws ClassNotFoundException, SQLException {
        long currentTimestamp = System.currentTimeMillis();
        Class.forName("com.impetus.fabric.jdbc.FabricDriver");
        File configFolder = new File("src/test/resources/blockchain-query");
        String configPath = configFolder.getAbsolutePath();
        Connection conn = DriverManager.getConnection("jdbc:fabric://" + configPath+":mychannel", "Impetus User", "");
        
        Statement stat = conn.createStatement();
        String createFuncQuery = "CREATE FUNCTION chncodeFunc" + currentTimestamp + " AS 'assettransfer' WITH VERSION '1.0'";
        stat.execute(createFuncQuery);
        String upgradeFuncQuery = "UPGRADE FUNCTION chncodeFunc" + currentTimestamp + " AS 'assettransfer' WITH VERSION '2.0'";
        stat.execute(upgradeFuncQuery);
    }


}
