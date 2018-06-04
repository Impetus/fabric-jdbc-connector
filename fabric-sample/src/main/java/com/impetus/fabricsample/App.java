package com.impetus.fabricsample;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

/**
 * Hello world!
 *
 */
public class App {
    
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        String configPath = System.getProperty("configPath");
        String channel = System.getProperty("channel");
        if(configPath == null || channel == null) {
            throw new RuntimeException("Set 'configPath' and 'channel' variables in system properties");
        }
        String menu = "Select any of the below operation\n"
                + "  1. Create asset schema\n"
                + "  2. Create chaincode\n"
                + "  3. Insert new schema change query\n"
                + "  4. Read last schema change query on table\n"
                + "  5. Read history of schema changes on table\n"
                + "  6. Read history of all schema changes of database\n";
        System.out.println(menu);
        Scanner scanner = new Scanner(System.in);
        int option = scanner.nextInt();
        if((option < 1) || (option > 6)) {
            throw new RuntimeException("Select option between 1 and 6");
        }
        Class.forName("com.impetus.fabric.jdbc.FabricDriver");
        Connection conn = DriverManager.getConnection("jdbc:fabric://" + configPath + ":" + channel, "Hari", "");
        if(option == 1) {
            createAsset(conn);
        } else if(option == 2) {
            createChaincode(conn);
        } else {
            System.out.println("Please enter the database name");
            String database = scanner.next();
            if(option == 6) {
                callHistoryForDatabase(conn, database);
            } else {
                System.out.println("Please enter the table name");
                String table = scanner.next();
                if(option == 4) {
                    callValueForKey(conn, database, table);
                } else if(option == 5) {
                    callHistoryForKey(conn, database, table);
                } else {
                    System.out.println("Please enter the query");
                    scanner.nextLine(); // Dummy call because scanner.next() method doesn't consume newline character
                    String query = scanner.nextLine();
                    insertAsset(conn, database, table, query);
                }
            }
        }
        scanner.close();
    }
    
    public static boolean createAsset(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        String createAssetQuery = "CREATE ASSET ETL_TRACKER ("
                + "database string,"
                + "table string,"
                + "query string,"
                + "time string)"
                + "WITH storage type CSV "
                + "Fields Delimited by ';' "
                + "Records Delimited by '\\n'";
        return stat.execute(createAssetQuery);
    }
    
    public static boolean createChaincode(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        String createFuncQuery = "CREATE CHAINCODE etltracker AS 'etltracker' WITH VERSION '1.0'";
        return stat.execute(createFuncQuery);
    }
    
    public static boolean insertAsset(Connection conn, String database, String table, String query) throws SQLException {
        Statement stat = conn.createStatement();
        String insertFuncQuery = String.format("insert into etltracker values('insert', '%s', '%s', '%s')", database, table, query);
        return stat.execute(insertFuncQuery);
    }
    
    public static void callValueForKey(Connection conn, String database, String table) throws SQLException {
        Statement stat = conn.createStatement();
        String callFuncQuery = String.format("CALL etltracker('query', '%s', '%s') AS ASSET ETL_TRACKER", database, table);
        ResultSet rs = stat.executeQuery(callFuncQuery);
        ResultSetMetaData metadata = rs.getMetaData();
        StringBuilder sb = new StringBuilder();
        for(int i = 1 ; i <= metadata.getColumnCount() ; i++) {
            sb.append(metadata.getColumnLabel(i) + " | ");
        }
        sb.delete(sb.length() - 3, sb.length());
        System.out.println(sb.toString());
        while(rs.next()) {
            System.out.println(rs.getString("database") + " | " + rs.getString("table") + " | " + rs.getString("query") + " | " + rs.getString("time"));
        }
    }
    
    public static void callHistoryForKey(Connection conn, String database, String table) throws SQLException {
        Statement stat = conn.createStatement();
        String callFuncQuery = String.format("CALL etltracker('queryHistory', '%s', '%s') AS ASSET ETL_TRACKER", database, table);
        if(stat.execute(callFuncQuery)) {
            ResultSet rs = stat.getResultSet();
            ResultSetMetaData metadata = rs.getMetaData();
            StringBuilder sb = new StringBuilder();
            for(int i = 1 ; i <= metadata.getColumnCount() ; i++) {
                sb.append(metadata.getColumnLabel(i) + " | ");
            }
            sb.delete(sb.length() - 3, sb.length());
            System.out.println(sb.toString());
            while(rs.next()) {
                System.out.println(rs.getString("database") + " | " + rs.getString("table") + " | " + rs.getString("query") + " | " + rs.getString("time"));
            }
        }
    }
    
    public static void callHistoryForDatabase(Connection conn, String database) throws SQLException {
        Statement stat = conn.createStatement();
        String callFuncQuery = String.format("CALL etltracker('queryDatabaseHistory', '%s') AS ASSET ETL_TRACKER", database);
        ResultSet rs = stat.executeQuery(callFuncQuery);
        ResultSetMetaData metadata = rs.getMetaData();
        StringBuilder sb = new StringBuilder();
        for(int i = 1 ; i <= metadata.getColumnCount() ; i++) {
            sb.append(metadata.getColumnLabel(i) + " | ");
        }
        sb.delete(sb.length() - 3, sb.length());
        System.out.println(sb.toString());
        while(rs.next()) {
            System.out.println(rs.getString("database") + " | " + rs.getString("table") + " | " + rs.getString("query") + " | " + rs.getString("time"));
        }
    }
    
    
}
