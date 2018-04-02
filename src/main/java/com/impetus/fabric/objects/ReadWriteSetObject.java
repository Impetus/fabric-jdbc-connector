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
