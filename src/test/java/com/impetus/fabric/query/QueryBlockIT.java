package com.impetus.fabric.query;


import junit.framework.TestCase;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.impetus.test.catagory.IntegrationTest;

import javax.print.attribute.standard.DateTimeAtProcessing;
import java.io.File;
import java.sql.*;
import java.util.Date;

@Category(IntegrationTest.class)
public class QueryBlockIT extends TestCase {

    @Test
    public void testFabricStatement() throws ClassNotFoundException, SQLException {
        Class.forName("com.impetus.fabric.jdbc.FabricDriver");
        File configFolder = new File("src/test/resources/blockchain-query");
        String configPath = configFolder.getAbsolutePath();
        Connection conn = DriverManager.getConnection("jdbc:fabric://" + configPath+":mychannel", "Swati Raj", "");
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("select * from block where blockNo=2"); // This is dummy query
        assert(rs.next());
    }

    @Test
    public void testFabricStatementWithOrderByAndGroupBy() throws ClassNotFoundException, SQLException {
        Class.forName("com.impetus.fabric.jdbc.FabricDriver");
        File configFolder = new File("src/test/resources/blockchain-query");
        String configPath = configFolder.getAbsolutePath();
        Connection conn = DriverManager.getConnection("jdbc:fabric://" + configPath+":mychannel", "Swati Raj", "");
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("select * from block where blockNo >= 2 and blockNo <= 5 groupby('blockno') order by blockNo desc");
        assert(rs.next());
    }


    // No need to Assert, test passed if didnt throw exception
    public void testCreateFunction() throws ClassNotFoundException, SQLException{
        long currentTimeStamp = System.currentTimeMillis();
        Class.forName("com.impetus.fabric.jdbc.FabricDriver");
        File configFolder = new File("src/test/resources/blockchain-query");
        String configPath = configFolder.getAbsolutePath();
        Connection conn = DriverManager.getConnection("jdbc:fabric://" + configPath+":mychannel", "Swati Raj", "");
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
        Connection conn = DriverManager.getConnection("jdbc:fabric://" + configPath+":mychannel", "Swati Raj", "");
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
        Connection conn = DriverManager.getConnection("jdbc:fabric://" + configPath+":mychannel", "Swati Raj", "");
        Statement stat = conn.createStatement();

        String createFuncQuery = "CREATE FUNCTION chncodefunc"+currentTimeStamp+" AS 'hyperledger/fabric/examples/chaincode/go/chaincode_example02' WITH VERSION '1.0'"
                + " WITH ARGS a, 500, b, 200";
        stat.execute(createFuncQuery);
        String insertQuery = "INSERT INTO chncodefunc"+currentTimeStamp+" VALUES(invoke, a, b, 20)";
        stat.execute(insertQuery);
    }

    //TODO Check this unit test, it should not throw null point exception after fix.
    //TODO This function should work even if there is no block number with id 200
    @Test
    public void testPhysicalQueryOptimization() throws ClassNotFoundException, SQLException {
        Class.forName("com.impetus.fabric.jdbc.FabricDriver");
        File configFolder = new File("src/test/resources/blockchain-query");
        String configPath = configFolder.getAbsolutePath();
        Connection conn = DriverManager.getConnection("jdbc:fabric://" + configPath+":mychannel", "Swati Raj", "");
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
        Connection conn = DriverManager.getConnection("jdbc:fabric://" + configPath+":mychannel", "Swati Raj", "");
        Statement stat = conn.createStatement();

        ResultSet rs = stat.executeQuery("select * from block blk where blockNo >= -2 and blockNo <= 1"); // This is dummy query

        assert(rs.next());
    }


}
