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

import java.lang.ref.WeakReference;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

import org.hyperledger.fabric.protos.common.Common.ChannelHeader;
import org.hyperledger.fabric.protos.common.Common.Envelope;
import org.hyperledger.fabric.protos.common.Common.Header;
import org.hyperledger.fabric.protos.common.Common.Payload;
import org.hyperledger.fabric.protos.common.Common.SignatureHeader;
import org.hyperledger.fabric.protos.msp.Identities.SerializedIdentity;
import org.hyperledger.fabric.protos.peer.FabricTransaction.ProcessedTransaction;
import org.hyperledger.fabric.protos.peer.FabricTransaction.Transaction;
import org.hyperledger.fabric.protos.peer.FabricTransaction.TransactionAction;
import org.hyperledger.fabric.sdk.TransactionInfo;

import com.google.protobuf.InvalidProtocolBufferException;
import com.impetus.blkch.BlkchnException;

public class TransactionDeserializer {

    private TransactionInfo transactionInfo;
    
    private WeakReference<Header> _header;
    
    private WeakReference<ChannelHeader> _channelHeader;
    
    private WeakReference<SignatureHeader> _signatureHeader;
    
    private WeakReference<SerializedIdentity> _serializedIdentity;
    
    public TransactionDeserializer(TransactionInfo transactionInfo) {
        this.transactionInfo = transactionInfo;
    }
    
    public String getTransactionId() {
        return transactionInfo.getTransactionID();
    }
    
    public int getHeaderType() {
        if(_channelHeader == null || _channelHeader.get() == null) {
            populateChannelHeader();
        }
        return _channelHeader.get().getType();
    }
    
    public int getMessageProtocolVersion() {
        if(_channelHeader == null || _channelHeader.get() == null) {
            populateChannelHeader();
        }
        return _channelHeader.get().getVersion();
    }
    
    public Timestamp getTimestamp() {
        if(_channelHeader == null || _channelHeader.get() == null) {
            populateChannelHeader();
        }
        long millis = (_channelHeader.get().getTimestamp().getSeconds() * 1000) + 
                (_channelHeader.get().getTimestamp().getNanos() / (1000 * 1000));
        return new Timestamp(millis);
    }
    
    public long getEpoch() {
        if(_channelHeader == null || _channelHeader.get() == null) {
            populateChannelHeader();
        }
        return _channelHeader.get().getEpoch();
    }
    
    public String getChannelId() {
        if(_channelHeader == null || _channelHeader.get() == null) {
            populateChannelHeader();
        }
        return _channelHeader.get().getChannelId();
    }
    
    public String getCreatorMSP() {
        if(_serializedIdentity == null || _serializedIdentity.get() == null) {
            populateSerializedIdentity();
        }
        return _serializedIdentity.get().getMspid();
    }
    
    public String getCreatorSignature() {
        if(_serializedIdentity == null || _serializedIdentity.get() == null) {
            populateSerializedIdentity();
        }
        return _serializedIdentity.get().getIdBytes().toStringUtf8();
    }
    
    public String getNonce() {
        if(_signatureHeader == null || _signatureHeader.get() == null) {
            populateSignatureHeader();
        }
        byte[] nonceBytes = _signatureHeader.get().getNonce().toByteArray();
        Byte[] nonceArr = new Byte[nonceBytes.length];
        Arrays.setAll(nonceArr, n -> nonceBytes[n]);
        return Arrays.asList(nonceArr).toString();
    }
    
    public List<TransactionAction> getTransactionActions() {
        ProcessedTransaction processedTransaction = transactionInfo.getProcessedTransaction();
        Envelope envelope = processedTransaction.getTransactionEnvelope();
        Payload payload;
        try {
            payload = Payload.parseFrom(envelope.getPayload());
            Transaction transaction = Transaction.parseFrom(payload.getData());
            return transaction.getActionsList();
        } catch (InvalidProtocolBufferException e) {
            throw new BlkchnException("Error creating object from ByteString", e);
        }
    }
    
    private void populateHeader() {
        ProcessedTransaction processedTransaction = transactionInfo.getProcessedTransaction();
        Envelope envelope = processedTransaction.getTransactionEnvelope();
        Payload payload;
        try {
            payload = Payload.parseFrom(envelope.getPayload());
        } catch (InvalidProtocolBufferException e) {
            throw new BlkchnException("Error creating object from ByteString", e);
        }
        Header header = payload.getHeader();
        _header = new WeakReference<>(header);
    }
    
    private void populateChannelHeader() {
        Header header = null;
        if(_header != null) {
            header = _header.get();
        }
        if(null == header) {
            populateHeader();
            header = _header.get();
        }
        ChannelHeader channelHeader;
        try {
            channelHeader = ChannelHeader.parseFrom(header.getChannelHeader());
        } catch (InvalidProtocolBufferException e) {
            throw new BlkchnException("Error creating object from ByteString", e);
        }
        _channelHeader = new WeakReference<>(channelHeader);
    }
    
    private void populateSignatureHeader() {
        Header header = null;
        if(_header != null) {
            header = _header.get();
        }
        if(null == header) {
            populateHeader();
            header = _header.get();
        }
        SignatureHeader signatureHeader;
        try {
            signatureHeader = SignatureHeader.parseFrom(header.getSignatureHeader());
        } catch (InvalidProtocolBufferException e) {
            throw new BlkchnException("Error creating object from ByteString", e);
        }
        _signatureHeader = new WeakReference<>(signatureHeader);
    }
    
    private void populateSerializedIdentity() {
        SignatureHeader signatureHeader = null;
        if(_signatureHeader != null) {
            signatureHeader = _signatureHeader.get();
        }
        if(null == signatureHeader) {
            populateSignatureHeader();
            signatureHeader = _signatureHeader.get();
        }
        SerializedIdentity serializedIdentity;
        try {
            serializedIdentity = SerializedIdentity.parseFrom(signatureHeader.getCreator());
        } catch (InvalidProtocolBufferException e) {
            throw new BlkchnException("Error creating object from ByteString", e);
        }
        _serializedIdentity = new WeakReference<>(serializedIdentity);
    }
}
