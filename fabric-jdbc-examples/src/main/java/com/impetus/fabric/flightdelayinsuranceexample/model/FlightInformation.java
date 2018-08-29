package com.impetus.fabric.flightdelayinsuranceexample.model;

public class FlightInformation {
    public FlightInformation(){
        this.ID="6E23";
        this.Rectype="FLIGHT";
        this.Source="source23";
        this.Destn="dest23";
        this.StandardDepartureTime="2018-07-25T16:45:00";
        this.ActualDepartureTime="NA";
    }

    public String ID;
    public String Rectype;
    public String Source;
    public String Destn;
    public String StandardDepartureTime;
    public String ActualDepartureTime;
}
