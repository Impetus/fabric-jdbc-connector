package com.impetus.fabric.flightdelayinsuranceexample.model;

public class PolicyInformation {

    public PolicyInformation() {
        this.PolicyID = "101855";
        this.RecType = "USRPOLICY";
        this.UserID = "usera@usera.com";
        this.PolicyTypeID = "101";
        this.FlightID = "6E23";
        this.Source = "source23";
        this.Destn = "dest23";
        this.StandardTimeOfDeparture = "2018-07-25T16:45:00";
        this.DateOfTravel = "2018-07-25";
        this.ClaimStatus = "NA";
    }

    public String PolicyID;
    public String RecType;
    public String UserID;
    public String PolicyTypeID;
    public String FlightID;
    public String Source;
    public String Destn;
    public String StandardTimeOfDeparture;
    public String DateOfTravel;
    public String ClaimStatus;
}
