package com.impetus.fabric.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

public class App {

    public static void main(String[] args) {
        String configPath = System.getProperty("configPath");
        String channel = System.getProperty("channel");
        if(configPath == null || channel == null) {
            throw new RuntimeException("Set 'configPath' and 'channel' variables in system properties");
        }
        String menu = "Select any of the below operation\n"
                + "  1. Create chaincode\n"
                + "  2. Transfer Asset\n"
                + "  3. Query Asset Details\n";
        System.out.println(menu);
        try(Scanner scanner = new Scanner(System.in)) {
            int option = scanner.nextInt();
            if((option < 1) || (option > 3)) {
                throw new RuntimeException("Select option between 1 and 3");
            }
            Class.forName("com.impetus.fabric.jdbc.FabricDriver");
            Connection conn = DriverManager.getConnection("jdbc:fabric://" + configPath + ":" + channel, "admin", "adminpw");
            if(option == 1) {
                createChaincode(conn);
                System.out.println("Chaincode created successfully");
            } else {
                System.out.println("Please enter Asset ID");
                int assetId = scanner.nextInt();
                if(option ==2) {
                    System.out.println("Please enter current participant ID");
                    int fromParticipantId = scanner.nextInt();
                    System.out.println("Please enter target participant ID");
                    int toParticipantId = scanner.nextInt();
                    transferAsset(conn, assetId, fromParticipantId, toParticipantId);
                    System.out.println("Asset transferred successfully");
                } else {
                    queryAsset(conn, assetId);
                }
            }
        } catch(Exception e) {
            System.out.println("ERROR: " + e.getMessage());
        }
    }
    
    private static boolean createChaincode(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        String createFuncQuery = "CREATE CHAINCODE assettransfer AS 'assettransfer' WITH VERSION '1.0'";
        return stat.execute(createFuncQuery);
    }
    
    private static void queryAsset(Connection conn, int assetId) throws SQLException {
        Statement stat = conn.createStatement();
        String queryFunc = "CALL assettransfer('getAsset', " + assetId + ")";
        ResultSet rs = stat.executeQuery(queryFunc);
        System.out.println(" data ");
        System.out.println("------");
        while(rs.next()) {
            System.out.println(rs.getString("data"));
        }
    }
    
    private static boolean transferAsset(Connection conn, int assetId, int from, int to) throws SQLException {
        Statement stat = conn.createStatement();
        String transferFunc = "INSERT INTO assettransfer VALUES('transferAsset', " + assetId + 
                ", " + from + ", " + to + ")";
        return stat.execute(transferFunc);
    }

}
