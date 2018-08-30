package com.impetus.fabric.flightdelayinsuranceexample;

import com.impetus.fabric.flightdelayinsuranceexample.model.FlightInformation;
import com.impetus.fabric.flightdelayinsuranceexample.model.PolicyInformation;
import com.impetus.fabric.flightdelayinsuranceexample.model.PolicyType;
import com.impetus.fabric.flightdelayinsuranceexample.model.User;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

public class InsuranceApp {


    public static void main(String[] args) throws Exception{
        String configPath = System.getProperty("configPath");
        String channelName = System.getProperty("channel");
        if(configPath == null || channelName == null){
            throw new Exception("config Path or channel is null");
        }

        String runApplicationOptions = "Choose from below options\n"+
                "1 - Install Chaincode and Run application \n"+
                "2 - ChainCode already Installed, directly run application \n"
                ;
        System.out.println(runApplicationOptions);
        Scanner scanner = new Scanner(System.in);
        int userOption = scanner.nextInt();
        if(userOption < 1 || userOption > 2){
            System.out.println("Selected option is invalid");
            return;
        }
        File srcFolder = new File("src/main/resources/flightdelayinsurance");
        String srcPath = srcFolder.getAbsolutePath();
        String destDirectory = configPath+"/src/flightdelayinsurance/";
        FileUtils.copyDirectory(new File(srcPath),new File(destDirectory));

        Class.forName("com.impetus.fabric.jdbc.FabricDriver");
        Connection conn = DriverManager.getConnection("jdbc:fabric://"+configPath+":"+channelName,"admin","adminpw");
        String chainCodeName = "flightDelayInsurance";
        if(userOption == 1){
            installAndInstiateChainCode(conn,chainCodeName);
            Thread.sleep(4000);
        }
        User usr = new User();
        PolicyInformation pinfo = new PolicyInformation();
        FlightInformation fInfo = new FlightInformation();
        PolicyType policyType = new PolicyType();
        savePolicy(policyType,conn,chainCodeName);
        registerUser(usr,conn,chainCodeName);
        Thread.sleep(4000);
        saveFlightInformationData(usr,fInfo,conn,chainCodeName);
        Thread.sleep(4000);
        saveUserPolicyInformationData(usr,pinfo,conn,chainCodeName);
        Thread.sleep(4000);

        //these options can be taken from end user. for now taking from class constructor.
        String getUserIdMessage = "Please enter registered user email address to show user data. Registered user email is "+ usr.Email +"\n";
        System.out.println(getUserIdMessage);
        scanner = new Scanner(System.in);
        String userEmailId = scanner.nextLine();
        if(displayUserClaimStatus(userEmailId,conn,chainCodeName)) {

            System.out.println("Please enter policyId to display policy Status. Register PolicyID is "+ pinfo.PolicyID);
            String policyId = scanner.nextLine();

            if(displayPolicyInformation(policyId,conn,chainCodeName)) {
                // update flight departure time
                FlightInformation flightInformation = new FlightInformation();
                String adminUserMenu = "Please Enter Actual Flight Departure Time. Schedule Flight departure Time was "+ flightInformation.StandardDepartureTime;
                System.out.println(adminUserMenu);
                String selectedOption = scanner.nextLine();

                flightInformation.ActualDepartureTime = selectedOption;
                updateActualFlightDetails(flightInformation,conn,chainCodeName);

                System.out.println("below is updated claim status");
                Thread.sleep(5000);
                displayPolicyInformation(policyId,conn,chainCodeName);
            }
        }
    }

    private static boolean displayPolicyInformation(String policyID, Connection conn, String chainCodeName){
        Boolean recFound = false;
        try{
            Statement stat = conn.createStatement();
            String queryString = "CALL "+chainCodeName+"('qGetQuery','USRPOLICY','" + policyID + "')";
            ResultSet rs = stat.executeQuery(queryString);
            if(rs.next()){
                System.out.println(rs.getString(1));
                recFound = true;
            }
            else{
                System.out.println("Policy Information not found for this policyID");
            }
        } catch(Exception e){
            System.out.println(e);
            System.out.println("Policy Information not found for this policyID");
        }
        return recFound;
    }

    private static boolean displayUserClaimStatus(String emailId, Connection conn, String chainCodeName) {
        Boolean recFound = false;
        try {
            Statement stat = conn.createStatement();
            String queryString = "CALL "+chainCodeName+"('qGetQuery','USER','" + emailId + "')";
            ResultSet rs = stat.executeQuery(queryString);
            if (rs.next()) {
                System.out.println(rs.getString(1));
                recFound = true;
            } else {
                System.out.println("User not found for this id:" + emailId);
            }
        } catch (Exception e) {
            System.out.println(e);
            System.out.println("User not found for this id:" + emailId);
        }
        return recFound;
    }


    private static void installAndInstiateChainCode(Connection conn, String chainCodeName) throws SQLException {
        Statement stat = conn.createStatement();
        String createChainCode = "CREATE Chaincode "+chainCodeName+" as 'flightdelayinsurance' WITH VERSION '1.0'";
        stat.execute(createChainCode);
    }


    private static boolean registerUser(User usr, Connection conn, String chainCodeName) throws SQLException {
        Statement stat = conn.createStatement();
        String transferFunc = "INSERT INTO "+chainCodeName+" VALUES('iCreateUser', '" +
                usr.Email
                + "', '"+ usr.RecType
                +"', '" + usr.FirstName
                + "', '"+ usr.LastName
                + "', '"+ usr.Address
                + "', '"+ usr.Dob
                + "', '"+ usr.Password
                + "')";
        try{
            boolean ret  = stat.execute(transferFunc);
            System.out.println("default user registration completed.");
        }
        catch (Exception e){
            System.out.println(e);
        }
        return true;
    }

    private static void saveUserPolicyInformationData(User usr, PolicyInformation policy, Connection conn, String chainCodeName) throws SQLException {
        Statement stat = conn.createStatement();
        String transferFunc = "INSERT INTO "+chainCodeName+" VALUES('iSelectPolicy', '" +
                policy.PolicyID // Auto Generated Number from System.
                + "', '"+ policy.RecType
                + "', '"+ usr.Email
                +"', '" + policy.PolicyTypeID // It will come by user selection
                + "', '"+ policy.FlightID
                + "', '"+ policy.Source
                + "', '"+ policy.Destn
                + "', '"+ policy.StandardTimeOfDeparture
                + "', '"+ policy.DateOfTravel
                + "', '"+ policy.ClaimStatus // Default NA
                + "')";
        try{
            boolean ret  = stat.execute(transferFunc);
            System.out.println("default PolicyInformationData Added");
        }
        catch (Exception e){
            System.out.println(e);
        }
    }

    private static void saveFlightInformationData(User usr, FlightInformation flightInformation, Connection conn, String chainCodeName) throws SQLException {
        Statement stat = conn.createStatement();
        String transferFunc = "INSERT INTO "+chainCodeName+" VALUES('iCreateFlightRec', '" +
                flightInformation.ID
                + "', '"+ flightInformation.Rectype
                + "', '"+ flightInformation.Source
                +"', '" + flightInformation.Destn
                + "', '"+ flightInformation.StandardDepartureTime
                + "', '"+ flightInformation.ActualDepartureTime // Default NA
                + "')";
        try{
            boolean ret  = stat.execute(transferFunc);
            System.out.println("default FlightInformationData Added");
        }
        catch (Exception e){
            System.out.println(e);
        }
    }


    private static void savePolicy(PolicyType policyType, Connection conn, String chainCodeName) throws SQLException {
        try {
            Statement stat = conn.createStatement();
            String transferFunc = "INSERT INTO " + chainCodeName + " VALUES('iCreatePolicy', '" +
                    policyType.ID
                    + "', '" + policyType.RecType
                    + "', '" + policyType.Name
                    + "', '" + policyType.Details
                    + "')";

            boolean ret = stat.execute(transferFunc);
            System.out.println("default policy added");
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    private static void updateActualFlightDetails(FlightInformation flightInformation, Connection conn,String chainCodeName) throws SQLException{
        Statement stat = conn.createStatement();
        String transferFunc = "INSERT INTO " + chainCodeName + " VALUES('iUpdateATD', '" +
                flightInformation.ID
                + "', '" + flightInformation.StandardDepartureTime.split("T")[0]
                + "', '" + flightInformation.ActualDepartureTime.split("T")[1]
                + "')";
        try {
            boolean ret = stat.execute(transferFunc);
            System.out.println("ActualFlightDetails updated");
        } catch (Exception e) {
            System.out.println(e);
        }

    }
}
