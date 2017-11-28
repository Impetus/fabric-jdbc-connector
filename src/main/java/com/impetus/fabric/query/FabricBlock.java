package com.impetus.fabric.query;

import java.util.ArrayList;
import java.util.List;

public class FabricBlock {

    public long getBlockId() {
        return blockId;
    }

    public void setBlockId(long blockId) {
        this.blockId = blockId;
    }

    public String getBlockHash() {
        return blockHash;
    }

    public void setBlockHash(String blockHash) {
        this.blockHash = blockHash;
    }

    public String getPrevBlockHash() {
        return prevBlockHash;
    }

    public void setPrevBlockHash(String prevBlockHash) {
        this.prevBlockHash = prevBlockHash;
    }

    public int getTransacCount() {
        return transactionCount;
    }

    public void setTransacCount(int transactionCount) {
        this.transactionCount = transactionCount;
    }

    public String getChannelName() {
        return channelName;
    }

    public void setChannelName(String channelName) {
        this.channelName = channelName;
    }

    public List<Object> getRecordData() {
        // List<Object[]> record = new ArrayList<>();

        List<Object> returnList = new ArrayList<>();
        returnList.add(blockId);
        returnList.add(blockHash);
        returnList.add(prevBlockHash);
        returnList.add(channelName);
        returnList.add(transactionCount);

        // /record.add(returnList.toArray());
        return returnList;
    }

    private long blockId;

    private String blockHash;

    private String prevBlockHash;

    private int transactionCount;

    private String channelName;

}
