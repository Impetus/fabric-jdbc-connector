package com.impetus.fabric.objects;

import org.hyperledger.fabric.protos.ledger.rwset.kvrwset.KvRwset.KVRead;
import org.hyperledger.fabric.protos.ledger.rwset.kvrwset.KvRwset.KVWrite;
import org.hyperledger.fabric.protos.ledger.rwset.kvrwset.KvRwset.RangeQueryInfo;

public class ReadWriteSetDeserializer {

    private KVRead read;
    
    private RangeQueryInfo rangeQueryInfo;
    
    private KVWrite write;
    
    public ReadWriteSetDeserializer(KVRead read) {
        this.read = read;
    }
    
    public ReadWriteSetDeserializer(RangeQueryInfo rangeQueryInfo) {
        this.rangeQueryInfo = rangeQueryInfo;
    }
    
    public ReadWriteSetDeserializer(KVWrite write) {
        this.write = write;
    }
    
    public String getReadKey() {
        return read == null ? null : read.getKey();
    }
    
    public Long getReadBlockNo() {
        return read == null ? null : read.getVersion().getBlockNum();
    }
    
    public Long getReadTxNum() {
        return read == null ? null : read.getVersion().getTxNum();
    }
    
    public String getRangeQueryStartKey() {
        return rangeQueryInfo == null ? null : rangeQueryInfo.getStartKey();
    }
    
    public String getRangeQueryEndKey() {
        return rangeQueryInfo == null ? null : rangeQueryInfo.getEndKey();
    }
    
    public Boolean getRangeQueryItrExausted() {
        return rangeQueryInfo == null ? null : rangeQueryInfo.getItrExhausted();
    }
    
    public String getRangeQueryReadsInfo() {
        return rangeQueryInfo == null ? null : rangeQueryInfo.getReadsInfoCase().name();
    }
    
    public String getWriteKey() {
        return (write == null || write.getKey() == null) ? null : write.getKey().replaceAll("[^\\p{Print}]", "");
    }
    
    public Boolean getIsDelete() {
        return write == null ? null : write.getIsDelete();
    }
    
    public String getWriteValue() {
        return (write == null || !write.getValue().isValidUtf8()) ? null : write.getValue().toStringUtf8().replaceAll("[^\\p{Print}]", "");
    }
}
