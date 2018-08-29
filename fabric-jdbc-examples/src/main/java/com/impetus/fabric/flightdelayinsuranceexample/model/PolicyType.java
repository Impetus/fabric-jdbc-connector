package com.impetus.fabric.flightdelayinsuranceexample.model;

public class PolicyType {
    public PolicyType() {
        this.ID = 101;
        this.RecType = "POLICY";
        this.Name = "Policy 101";
        this.Details = "Covering Flight delay between 2-3 Hrs";
    }
    public long ID;
    public String RecType;
    public String Name;
    public String Details;

}
