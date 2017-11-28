/*******************************************************************************
* * Copyright 2017 Impetus Infotech.
* *
* * Licensed under the Apache License, Version 2.0 (the "License");
* * you may not use this file except in compliance with the License.
* * You may obtain a copy of the License at
* *
* * http://www.apache.org/licenses/LICENSE-2.0
* *
* * Unless required by applicable law or agreed to in writing, software
* * distributed under the License is distributed on an "AS IS" BASIS,
* * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* * See the License for the specific language governing permissions and
* * limitations under the License.
******************************************************************************/
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
