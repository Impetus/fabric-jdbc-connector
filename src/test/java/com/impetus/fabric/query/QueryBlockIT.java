package com.impetus.fabric.query;


import com.impetus.blkch.sql.parser.AbstractAssetManager;
import com.impetus.fabric.jdbc.FabricResultSet;
import com.impetus.fabric.parser.AssetSchema;
import junit.framework.TestCase;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.impetus.test.catagory.IntegrationTest;

import javax.print.attribute.standard.DateTimeAtProcessing;
import java.io.File;
import java.sql.*;
import java.util.Date;
import java.util.Properties;

@Category(IntegrationTest.class)
public class QueryBlockIT extends TestCase {

    @Test
    public void testFabricStatement() throws ClassNotFoundException, SQLException {
        Class.forName("com.impetus.fabric.jdbc.FabricDriver");
        File configFolder = new File("src/test/resources/blockchain-query");
        String configPath = configFolder.getAbsolutePath();
        Connection conn = DriverManager.getConnection("jdbc:fabric://" + configPath+":mychannel", "Impetus User", "");
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("select * from block where block_no = 2"); // This is dummy query
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
        String createFuncQuery = "CREATE FUNCTION chncodefunc"+currentTimeStamp+" AS 'hyperledger/fabric/examples/chaincode/go/chaincode_example02' WITH VERSION '1.0'"
                + " WITH ARGS 'a', 500, 'b', 200";

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
        String createFuncQuery = "CREATE FUNCTION chncodefunc"+currentTimeStamp+" AS 'hyperledger/fabric/examples/chaincode/go/chaincode_example02' WITH VERSION '1.0'"
                + " WITH ARGS 'a', 500, 'b', 200";
        System.out.println(stat.execute(createFuncQuery));

        String callQuery = "CALL chncodefunc"+currentTimeStamp+"('invoke', 'a', 'b', 20)";
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

        String createFuncQuery = "CREATE FUNCTION chncodefunc"+currentTimeStamp+" AS 'hyperledger/fabric/examples/chaincode/go/chaincode_example02' WITH VERSION '1.0'"
                + " WITH ARGS 'a', 500, 'b', 200";
        stat.execute(createFuncQuery);
        String insertQuery = "INSERT INTO chncodefunc"+currentTimeStamp+" VALUES('invoke', 'a', 'b', 20)";
        stat.execute(insertQuery);
    }



    @Test
    public void testQueryWithExpectedEmptyReturn() throws ClassNotFoundException, SQLException {
        Class.forName("com.impetus.fabric.jdbc.FabricDriver");
        File configFolder = new File("src/test/resources/blockchain-query");
        String configPath = configFolder.getAbsolutePath();
        Connection conn = DriverManager.getConnection("jdbc:fabric://" + configPath+":mychannel", "Impetus User", "");
        Statement stat = conn.createStatement();

        ResultSet rs = stat.executeQuery("select * from block blk where block_no >= -2 and block_no <= 1"); // This is dummy query

        assert(rs.next());
    }


}
