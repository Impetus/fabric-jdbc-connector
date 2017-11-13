package com.impetus.fabric.query;

import java.util.ArrayList;
import java.util.List;

public class FabricTransaction
{

    public String getTransaction_id()
    {
        return transaction_id;
    }

    public void setTransaction_id(String transaction_id)
    {
        this.transaction_id = transaction_id;
    }

    public String getChannel_id()
    {
        return channel_id;
    }

    public void setChannel_id(String channel_id)
    {
        this.channel_id = channel_id;           
    }

    public int getTransaction_status()
    {
        return transaction_status;
    }

    public void setTransaction_status(int transaction_status)
    {
        this.transaction_status = transaction_status;
    }

    public String getTransaction_args()
    {
        return transaction_args;
    }

    public void setTransaction_args(String transaction_args)
    {
        this.transaction_args = transaction_args;
    }

    public String getEndorser_id()
    {
        return endorser_id;
    }

    public void setEndorser_id(String endorser_id)
    {
        this.endorser_id = endorser_id;
    }

    public String getChaincode_name()
    {
        return chaincode_name;
    }

    public void setChaincode_name(String chaincode_name)
    {
        this.chaincode_name = chaincode_name;
    }

    public String getTrans_read_key()
    {
        return trans_read_key;
    }

    public void setTrans_read_key(String trans_read_key)
    {
        this.trans_read_key = trans_read_key;
    }

    public String getTrans_write_key()
    {
        return trans_write_key;
    }

    public void setTrans_write_key(String trans_write_key)
    {
        this.trans_write_key = trans_write_key;
    }

    public List<Object[]> getRecordData()
    {
        List<Object[]> record = new ArrayList<>();

        List<Object> returnList = new ArrayList<>();
        returnList.add(transaction_id);
        returnList.add(channel_id);
        returnList.add(transaction_status);
        returnList.add(transaction_args);
        returnList.add(endorser_id);
        returnList.add(chaincode_name);
        returnList.add(trans_read_key);
        returnList.add(trans_write_key);

        record.add(returnList.toArray());
        return record;
    }

    private String transaction_id;

    private String channel_id;

    private int transaction_status;

    private String transaction_args;

    private String endorser_id;

    private String chaincode_name;

    private String trans_read_key;

    private String trans_write_key;

}
