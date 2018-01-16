package com.impetus.fabric.query;


import junit.framework.TestCase;
import org.junit.Test;

import java.io.File;
import java.sql.*;

public class QueryBlockIT extends TestCase {

    @Test
    public void testFabricStatement() throws ClassNotFoundException, SQLException {
        Class.forName("com.impetus.fabric.jdbc.FabricDriver");
        File configFolder = new File("src/test/resources/blockchain-query");
        String configPath = configFolder.getAbsolutePath();
        Connection conn = DriverManager.getConnection("jdbc:fabric://" + configPath+":mychannel", "Swati Raj", "");
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("select * from block"); // This is dummy query
        assert(rs != null);
    }


    public void testCreateFunction() throws ClassNotFoundException, SQLException{
        Class.forName("com.impetus.fabric.jdbc.FabricDriver");
        File configFolder = new File("src/test/resources/blockchain-query");
        String configPath = configFolder.getAbsolutePath();
        Connection conn = DriverManager.getConnection("jdbc:fabric://" + configPath+":mychannel", "Swati Raj", "");
        Statement stat = conn.createStatement();
        String createFuncQuery = "CREATE FUNCTION chncodefunc AS 'hyperledger/fabric/examples/chaincode/go/chaincode_example02' WITH VERSION '1.0'"
                + " WITH ARGS a, 500, b, 200";
        System.out.println(stat.execute(createFuncQuery));
    }

    public void testCallFunction() throws ClassNotFoundException, SQLException{
        Class.forName("com.impetus.fabric.jdbc.FabricDriver");
        File configFolder = new File("src/test/resources/blockchain-query");
        String configPath = configFolder.getAbsolutePath();
        Connection conn = DriverManager.getConnection("jdbc:fabric://" + configPath+":mychannel", "Swati Raj", "");
        Statement stat = conn.createStatement();
        String createFuncQuery = "CREATE FUNCTION chncodefunc AS 'hyperledger/fabric/examples/chaincode/go/chaincode_example02' WITH VERSION '1.0'"
                + " WITH ARGS a, 500, b, 200";
        System.out.println(stat.execute(createFuncQuery));

        String callQuery = "CALL chncodefunc(invoke, a, b, 20)";
        System.out.println(stat.execute(callQuery));

    }

    public void testInsertFunction() throws ClassNotFoundException, SQLException{
        Class.forName("com.impetus.fabric.jdbc.FabricDriver");
        File configFolder = new File("src/test/resources/blockchain-query");
        String configPath = configFolder.getAbsolutePath();
        Connection conn = DriverManager.getConnection("jdbc:fabric://" + configPath+":mychannel", "Swati Raj", "");
        Statement stat = conn.createStatement();

        String createFuncQuery = "CREATE FUNCTION chncodefunc AS 'hyperledger/fabric/examples/chaincode/go/chaincode_example02' WITH VERSION '1.0'"
                + " WITH ARGS a, 500, b, 200";
        System.out.println(stat.execute(createFuncQuery));
        String insertQuery = "INSERT INTO chncodefunc VALUES(invoke, a, b, 20)";
        System.out.println(stat.execute(insertQuery));
    }




}
