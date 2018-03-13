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
        ResultSet rs = stat.executeQuery("select * from block where blockNo = 2"); // This is dummy query
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


    //TODO it will fail for now, once having with function logic added it will work.
    @Test
    public void testFabricStatementWithGroupByAndHaving() throws ClassNotFoundException, SQLException {
        Class.forName("com.impetus.fabric.jdbc.FabricDriver");
        File configFolder = new File("src/test/resources/blockchain-query");
        String configPath = configFolder.getAbsolutePath();
        Connection conn = DriverManager.getConnection("jdbc:fabric://" + configPath+":mychannel", "Impetus User", "");
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("select blockNo,sum(blockNo) from block where blockNo = 2 group by blockNo having sum(blockNo) = 1"); // This is dummy query
        assert(rs.next());
    }


    @Test
    public void testFabricStatementWithOrderByAndGroupBy() throws ClassNotFoundException, SQLException {
        Class.forName("com.impetus.fabric.jdbc.FabricDriver");
        File configFolder = new File("src/test/resources/blockchain-query");
        String configPath = configFolder.getAbsolutePath();
        Connection conn = DriverManager.getConnection("jdbc:fabric://" + configPath+":mychannel", "Impetus User", "");
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("select blockNo, sum(blockNo) from block where blockNo >= 2 and blockNo <= 5  group by blockNo order by blockNo DESC ");
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
        ResultSet rs = stat.executeQuery("select * from block where (blockNo = 2 or blockNo = 3) and (blockNo =4 or blockNo = 5)");
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
                + " WITH ARGS a, 500, b, 200";

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
                + " WITH ARGS a, 500, b, 200";
        System.out.println(stat.execute(createFuncQuery));

        String callQuery = "CALL chncodefunc"+currentTimeStamp+"(invoke, a, b, 20)";
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
                + " WITH ARGS a, 500, b, 200";
        stat.execute(createFuncQuery);
        String insertQuery = "INSERT INTO chncodefunc"+currentTimeStamp+" VALUES(invoke, 'a', 'b', '20')";
        stat.execute(insertQuery);
    }

    //TODO Check this unit test, it should not throw null point exception after fix.
    //TODO This function should work even if there is no block number with id 200
    @Test
    public void testPhysicalQueryOptimization() throws ClassNotFoundException, SQLException {
        Class.forName("com.impetus.fabric.jdbc.FabricDriver");
        File configFolder = new File("src/test/resources/blockchain-query");
        String configPath = configFolder.getAbsolutePath();
        Connection conn = DriverManager.getConnection("jdbc:fabric://" + configPath+":mychannel", "Impetus User", "");
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("select * from block blk where blockNo = 200 And blockNo>=2 blockNo <=300"); // This is dummy query
        assert(rs.next());
    }


    //TODO After fix this function range should include negate number as well.
    @Test
    public void testQueryWithExpectedEmptyReturn() throws ClassNotFoundException, SQLException {
        Class.forName("com.impetus.fabric.jdbc.FabricDriver");
        File configFolder = new File("src/test/resources/blockchain-query");
        String configPath = configFolder.getAbsolutePath();
        Connection conn = DriverManager.getConnection("jdbc:fabric://" + configPath+":mychannel", "Impetus User", "");
        Statement stat = conn.createStatement();

        ResultSet rs = stat.executeQuery("select * from block blk where blockNo >= -2 and blockNo <= 1"); // This is dummy query

        assert(rs.next());
    }


}
