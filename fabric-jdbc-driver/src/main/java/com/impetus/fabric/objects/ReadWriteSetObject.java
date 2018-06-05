/*******************************************************************************
 * * Copyright 2018 Impetus Infotech.
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

package com.impetus.fabric.objects;

import java.util.List;
import java.util.Map;

import org.hyperledger.fabric.protos.ledger.rwset.kvrwset.KvRwset.KVRead;
import org.hyperledger.fabric.protos.ledger.rwset.kvrwset.KvRwset.KVWrite;
import org.hyperledger.fabric.protos.ledger.rwset.kvrwset.KvRwset.RangeQueryInfo;

public class ReadWriteSetObject {
    
    private long blockNo;
    
    private String transactionId;
    
    private String namespace;
    
    private ReadWriteSetDeserializer deserializer;
    
    public ReadWriteSetObject(long blockNo, String transactionId, String namespace, KVRead read) {
        this.blockNo = blockNo;
        this.transactionId = transactionId;
        this.namespace = namespace;
        this.deserializer = new ReadWriteSetDeserializer(read);
    }
    
    public ReadWriteSetObject(long blockNo, String transactionId, String namespace, RangeQueryInfo rangeQueryInfo) {
        this.blockNo = blockNo;
        this.transactionId = transactionId;
        this.namespace = namespace;
        this.deserializer = new ReadWriteSetDeserializer(rangeQueryInfo);
    }
    
    public ReadWriteSetObject(long blockNo, String transactionId, String namespace, KVWrite write) {
        this.blockNo = blockNo;
        this.transactionId = transactionId;
        this.namespace = namespace;
        this.deserializer = new ReadWriteSetDeserializer(write);
    }

    public long getBlockNo() {
        return blockNo;
    }

    public void setBlockNo(long blockNo) {
        this.blockNo = blockNo;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public ReadWriteSetDeserializer getDeserializer() {
        return deserializer;
    }

    public void setDeserializer(ReadWriteSetDeserializer deserializer) {
        this.deserializer = deserializer;
    }

}
