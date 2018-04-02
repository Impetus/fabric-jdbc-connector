package com.impetus.fabric.objects;

import org.hyperledger.fabric.sdk.TransactionInfo;

public class TransactionObject {

    private Long blockNo;
    
    private TransactionDeserializer deserializer;
    
    public TransactionObject(Long blockNo, TransactionInfo transactionInfo) {
        this.blockNo = blockNo;
        this.deserializer = new TransactionDeserializer(transactionInfo);
    }

    public Long getBlockNo() {
        return blockNo;
    }

    public void setBlockNo(Long blockNo) {
        this.blockNo = blockNo;
    }

    public TransactionDeserializer getDeserializer() {
        return deserializer;
    }

    public void setDeserializer(TransactionDeserializer transactionDeserializer) {
        this.deserializer = transactionDeserializer;
    }

}
