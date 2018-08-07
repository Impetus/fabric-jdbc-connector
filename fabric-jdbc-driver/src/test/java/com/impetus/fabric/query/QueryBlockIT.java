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
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.impetus.fabric.jdbc.FabricResultSet;
import junit.framework.TestCase;

import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.impetus.test.catagory.IntegrationTest;

@Category(IntegrationTest.class)
public class QueryBlockIT {

    @BeforeClass
    public static void beforeClass() throws ClassNotFoundException, SQLException {
        Class.forName("com.impetus.fabric.jdbc.FabricDriver");
        File configFolder = new File("src/test/resources/blockchain-query");
        String configPath = configFolder.getAbsolutePath();
        Connection conn = DriverManager.getConnection("jdbc:fabric://" + configPath+":mychannel", "admin", "adminpw");
        Statement stat = conn.createStatement();
        stat.execute("CREATE USER impadmin IDENTIFIED BY 'impadminpw' AFFILIATED TO org1.department1");
    }

    @Test
    public void testFabricStatement() throws ClassNotFoundException, SQLException {
        Class.forName("com.impetus.fabric.jdbc.FabricDriver");
        File configFolder = new File("src/test/resources/blockchain-query");
        String configPath = configFolder.getAbsolutePath();
        Connection conn = DriverManager.getConnection("jdbc:fabric://" + configPath+":mychannel", "impadmin", "impadminpw");
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("select * from block where block_no = 2");
        assert(rs.next());
    }

    @Test
    public void testDataframeSchemaInEmptyDataFrame() throws ClassNotFoundException, SQLException {
        Class.forName("com.impetus.fabric.jdbc.FabricDriver");
        File configFolder = new File("src/test/resources/blockchain-query");
        String configPath = configFolder.getAbsolutePath();
        Connection conn = DriverManager.getConnection("jdbc:fabric://" + configPath+":mychannel", "Impetus User", "");
        Statement stat = conn.createStatement();

        ResultSet rs_withAllColumn = stat.executeQuery("select * from block where block_no = 20000000");
        int columnCount = ((FabricResultSet)rs_withAllColumn).getMetaData().getColumnCount();
        assert(columnCount == 6);

        ResultSet rs_withFewColumn = stat.executeQuery("select previous_hash, block_no from block where block_no = 20000000");
        columnCount = ((FabricResultSet)rs_withFewColumn).getMetaData().getColumnCount();
        String col1 = ((FabricResultSet)rs_withFewColumn).getMetaData().getColumnName(1);
        String col2 = ((FabricResultSet)rs_withFewColumn).getMetaData().getColumnName(2);
        assert(col1.equals("previous_hash"));
        assert(col2.equals("block_no"));
        assert(columnCount == 2);


        ResultSet rs_limitZero = stat.executeQuery("select * from block where block_no = 2 limit 0");
        columnCount = ((FabricResultSet)rs_limitZero).getMetaData().getColumnCount();
        assert(columnCount == 6);

        ResultSet rs_fewColumnWithAlais = stat.executeQuery("select previous_hash as abc, block_no as def from block where block_no = 200000");
        columnCount = ((FabricResultSet)rs_fewColumnWithAlais).getMetaData().getColumnCount();
        col1 = ((FabricResultSet)rs_fewColumnWithAlais).getMetaData().getColumnName(1);
        col2 = ((FabricResultSet)rs_fewColumnWithAlais).getMetaData().getColumnName(2);
        assert(col1.equals("previous_hash"));
        assert(col2.equals("block_no"));
        assert(columnCount == 2);


        ResultSet rs_groupbyOrderBy = stat.executeQuery("select block_no, sum(block_no) from block where block_no >= 200000 and block_no <= 200001  group by block_no order by block_no DESC ");
        columnCount = ((FabricResultSet)rs_groupbyOrderBy).getMetaData().getColumnCount();
        col1 = ((FabricResultSet)rs_groupbyOrderBy).getMetaData().getColumnName(1);
        col2 = ((FabricResultSet)rs_groupbyOrderBy).getMetaData().getColumnName(2);
        assert(col1.equals("block_no"));
        assert(col2.equals("sum(block_no)"));
        assert(columnCount == 2);


        //Transaction Table Data
        ResultSet tr_withDataAllColumn = stat.executeQuery("select * from transaction where block_no = 10000000");
        columnCount = ((FabricResultSet)tr_withDataAllColumn).getMetaData().getColumnCount();
        assert(columnCount == 10);

        //With Wrong Column Name, it should throw exception
        try {
            ResultSet rs_limitZeroFewColumns_WrongColumn = stat.executeQuery("select previous_hash1, block_no1 from block where block_no = 2000000");
            assert(false);// If above call is not giving exception fail this test
        }
        catch(Exception e){
            //exception is Expected
            assert(true);
        }
    }


    @Test
    //Expecting empty Result set, if these call didnt throw exception, this test pass.
    public void testQueryWithWrongData() throws ClassNotFoundException, SQLException {
        Class.forName("com.impetus.fabric.jdbc.FabricDriver");
        File configFolder = new File("src/test/resources/blockchain-query");
        String configPath = configFolder.getAbsolutePath();
        Connection conn = DriverManager.getConnection("jdbc:fabric://" + configPath+":mychannel", "Impetus User", "");
        Statement stat = conn.createStatement();

        ResultSet rs = stat.executeQuery("select previous_hash from block where previous_hash = 'abcd82f37020778c5e99b6ebc6ce609624ba391e9adc5b72207e39fe'");
        assert(!rs.next());
        ResultSet rs_withTransactionID = stat.executeQuery("select * from transaction where transaction_id = 200000");
        assert(!rs_withTransactionID.next());
        ResultSet rs_read_write_set_WithTransactionID = stat.executeQuery("select * from read_write_set where transaction_id = 200000");
        assert(!rs_read_write_set_WithTransactionID.next());
        ResultSet rs_transaction_ActionWithTransactionID = stat.executeQuery("select * from transaction_action where transaction_id = 200000");
        assert(!rs_transaction_ActionWithTransactionID.next());
        //Test Range node and Direct API Node combination. It should return empty resultset
        ResultSet rs_BlockwithRangeNodeAndDataNode = stat.executeQuery("select * from block where  block_no > 0 and block_no <=2 and previous_hash = '200000'");
        assert(!rs_BlockwithRangeNodeAndDataNode.next());

        ResultSet rs_BlockwithRangeNodeOrDataNode = stat.executeQuery("select * from block where  block_no > 0 and block_no <=2 or previous_hash = '200000'");
        assert(rs_BlockwithRangeNodeOrDataNode.next());

    }

    @Test
    public void testSaveAssetToLocalDB() throws ClassNotFoundException, SQLException{

        Class.forName("com.impetus.fabric.jdbc.FabricDriver");
        File configFolder = new File("src/test/resources/blockchain-query");
        String configPath = configFolder.getAbsolutePath();
        Connection conn = DriverManager.getConnection("jdbc:fabric://" + configPath+":mychannel", "impadmin", "impadminpw");
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
        Connection conn = DriverManager.getConnection("jdbc:fabric://" + configPath+":mychannel", "impadmin", "impadminpw");
        Statement stat = conn.createStatement();
        String sql = "Drop ASSET user_asset1";
        stat.execute(sql);
    }


    @Test
    public void testSaveAssetWithFields() throws ClassNotFoundException, SQLException{

        Class.forName("com.impetus.fabric.jdbc.FabricDriver");
        File configFolder = new File("src/test/resources/blockchain-query");
        String configPath = configFolder.getAbsolutePath();
        Connection conn = DriverManager.getConnection("jdbc:fabric://" + configPath+":mychannel", "impadmin", "impadminpw");
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
        Connection conn = DriverManager.getConnection("jdbc:fabric://" + configPath+":mychannel", "impadmin", "impadminpw");
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
        Connection conn = DriverManager.getConnection("jdbc:fabric://" + configPath+":mychannel", "impadmin", "impadminpw");
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("select * from block where (block_no = 2 or block_no = 3) and (block_no =4 or block_no = 5)");
        assert(!rs.next());
    }


    // No need to Assert, test passed if didnt throw exception
    @Test
    public void testCreateFunction() throws ClassNotFoundException, SQLException{
        long currentTimeStamp = System.currentTimeMillis();
        Class.forName("com.impetus.fabric.jdbc.FabricDriver");
        File configFolder = new File("src/test/resources/blockchain-query");
        String configPath = configFolder.getAbsolutePath();
        Connection conn = DriverManager.getConnection("jdbc:fabric://" + configPath+":mychannel", "impadmin", "impadminpw");
        Statement stat = conn.createStatement();
        String createFuncQuery = "CREATE FUNCTION chncodefunc"+currentTimeStamp+" AS 'assettransfer' WITH VERSION '1.0'";
        stat.execute(createFuncQuery);
    }

    // No need to Assert, test passed if didnt throw exception
    @Test
    public void testCallFunction() throws ClassNotFoundException, SQLException{
        long currentTimeStamp = System.currentTimeMillis();
        Class.forName("com.impetus.fabric.jdbc.FabricDriver");
        File configFolder = new File("src/test/resources/blockchain-query");
        String configPath = configFolder.getAbsolutePath();
        Connection conn = DriverManager.getConnection("jdbc:fabric://" + configPath+":mychannel", "impadmin", "impadminpw");
        Statement stat = conn.createStatement();
        String createFuncQuery = "CREATE FUNCTION chncodefunc"+currentTimeStamp+" AS 'assettransfer' WITH VERSION '1.0'";
        System.out.println(stat.execute(createFuncQuery));

        String callQuery = "CALL chncodefunc"+currentTimeStamp+"('getAsset', 1000)";
        stat.execute(callQuery);


    }

    // No need to Assert, test passed if didnt throw exception
    @Test
    public void testInsertFunction() throws ClassNotFoundException, SQLException{
        Class.forName("com.impetus.fabric.jdbc.FabricDriver");
        File configFolder = new File("src/test/resources/blockchain-query");
        String configPath = configFolder.getAbsolutePath();
        Connection conn = DriverManager.getConnection("jdbc:fabric://" + configPath+":mychannel", "impadmin", "impadminpw");
        Statement stat = conn.createStatement();

        String createFuncQuery = "CREATE FUNCTION chncodefunc_testInsertFunction AS 'assettransfer' WITH VERSION '1.0'";
        stat.execute(createFuncQuery);
        String insertQuery = "INSERT INTO chncodefunc_testInsertFunction VALUES('transferAsset', 1001, 2001, 2002)";
        stat.execute(insertQuery);
    }



    @Test
    public void testQueryWithExpectedEmptyReturn() throws ClassNotFoundException, SQLException {
        Class.forName("com.impetus.fabric.jdbc.FabricDriver");
        File configFolder = new File("src/test/resources/blockchain-query");
        String configPath = configFolder.getAbsolutePath();
        Connection conn = DriverManager.getConnection("jdbc:fabric://" + configPath+":mychannel", "impadmin", "impadminpw");
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
        Connection conn = DriverManager.getConnection("jdbc:fabric://" + configPath+":mychannel", "impadmin", "impadminpw");
        
        Statement stat = conn.createStatement();
        String createFuncQuery = "CREATE FUNCTION chncodeFunc" + currentTimestamp + " AS 'assettransfer' WITH VERSION '1.0'";
        stat.execute(createFuncQuery);
        String upgradeFuncQuery = "UPGRADE FUNCTION chncodeFunc" + currentTimestamp + " AS 'assettransfer' WITH VERSION '2.0'";
        stat.execute(upgradeFuncQuery);
    }

    @Test
    public void testInsertMultiOrg() throws ClassNotFoundException, SQLException, IOException {
        Class.forName("com.impetus.fabric.jdbc.FabricDriver");
        File configFolder = new File("src/test/resources/blockchain-query-multi-org");
        String configPath = configFolder.getAbsolutePath();
        Connection conn = DriverManager.getConnection("jdbc:fabric://" + configPath+":mychannel", "admin", "adminpw");
        Statement stat = conn.createStatement();
        String installQuery = "CREATE FUNCTION chncodefunc_testInsertMultiOrg AS 'assettransfer' WITH VERSION '1.0' INSTALL ONLY";
        stat.execute(installQuery);
        FileUtils.copyFile(new File(configPath, "config_org2.properties"), new File(configPath, "config.properties"));
        Connection conn2 = DriverManager.getConnection("jdbc:fabric://" + configPath +":mychannel", "admin", "adminpw");
        Statement stat2 = conn2.createStatement();
        stat2.execute(installQuery);
        String endorsementFilePath = new File("src/test/resources/two_org_endorsement_policy.yaml").getAbsolutePath();
        stat2.execute("CREATE CHAINCODE chncodefunc_testInsertMultiOrg AS 'assettransfer' "
                + "WITH VERSION '1.0' WITH ENDORSEMENT POLICY FILE '"+ endorsementFilePath + "' "
                + "INSTANTIATE ONLY");
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            // Ignore
        }
        FileUtils.copyFile(new File(configPath, "config_org1.properties"), new File(configPath, "config.properties"));
        Connection conn3 = DriverManager.getConnection("jdbc:fabric://" + configPath+":mychannel", "impadmin", "impadminpw");
        Statement stat3 = conn3.createStatement();
        String insertQuery = "INSERT INTO chncodefunc_testInsertMultiOrg VALUES('transferAsset', 1001, 2001, 2002)";
        stat3.execute(insertQuery);
    }


}
