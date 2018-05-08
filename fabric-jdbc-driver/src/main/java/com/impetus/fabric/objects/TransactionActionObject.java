package com.impetus.fabric.objects;

import org.hyperledger.fabric.protos.peer.FabricTransaction.TransactionAction;

public class TransactionActionObject {

    private long blockNo;
    
    private String transactionId;
    
    private TransactionActionDeserializer deserializer;
    
    public TransactionActionObject(long blockNo, String transactionId, TransactionAction transactionAction) {
        this.blockNo = blockNo;
        this.transactionId = transactionId;
        this.deserializer = new TransactionActionDeserializer(transactionAction);
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

    public TransactionActionDeserializer getDeserializer() {
        return deserializer;
    }

    public void setDeserializer(TransactionActionDeserializer transactionActionDeserializer) {
        this.deserializer = transactionActionDeserializer;
    }
    
}
