package com.impetus.fabric.objects;

import java.lang.ref.WeakReference;
import java.sql.Array;
import java.util.List;

import org.hyperledger.fabric.protos.ledger.rwset.Rwset.TxReadWriteSet;
import org.hyperledger.fabric.protos.msp.Identities.SerializedIdentity;
import org.hyperledger.fabric.protos.peer.Chaincode.ChaincodeInput;
import org.hyperledger.fabric.protos.peer.Chaincode.ChaincodeInvocationSpec;
import org.hyperledger.fabric.protos.peer.FabricProposal.ChaincodeAction;
import org.hyperledger.fabric.protos.peer.FabricProposal.ChaincodeProposalPayload;
import org.hyperledger.fabric.protos.peer.FabricProposalResponse.Endorsement;
import org.hyperledger.fabric.protos.peer.FabricProposalResponse.ProposalResponsePayload;
import org.hyperledger.fabric.protos.peer.FabricTransaction.ChaincodeActionPayload;
import org.hyperledger.fabric.protos.peer.FabricTransaction.ChaincodeEndorsedAction;
import org.hyperledger.fabric.protos.peer.FabricTransaction.TransactionAction;
import org.json.simple.JSONObject;

import com.google.protobuf.InvalidProtocolBufferException;
import com.impetus.blkch.BlkchnException;
import com.impetus.fabric.jdbc.FabricArray;

public class TransactionActionDeserializer {

    private TransactionAction transactionAction;
    
    private WeakReference<ChaincodeActionPayload> _chaincodeActionPayload;
    
    private WeakReference<ChaincodeInvocationSpec> _chaincodeInvocationSpec;
    
    private WeakReference<ChaincodeAction> _chaincodeAction;
    
    private WeakReference<TxReadWriteSet> _txReadWriteSet;
    
    public TransactionActionDeserializer(TransactionAction transactionAction) {
        this.transactionAction = transactionAction;
    }
    
    public String getIdGenerationAlg() {
        if(_chaincodeInvocationSpec == null || _chaincodeInvocationSpec.get() == null) {
            populateChaincodeInvocationSpec();
        }
        return _chaincodeInvocationSpec.get().getIdGenerationAlg();
    }
    
    public String getChaincodeType() {
        if(_chaincodeInvocationSpec == null || _chaincodeInvocationSpec.get() == null) {
            populateChaincodeInvocationSpec();
        }
        return _chaincodeInvocationSpec.get().getChaincodeSpec().getType().name();
    }
    
    public String getChaincodeName() {
        if(_chaincodeInvocationSpec == null || _chaincodeInvocationSpec.get() == null) {
            populateChaincodeInvocationSpec();
        }
        return _chaincodeInvocationSpec.get().getChaincodeSpec().getChaincodeId().getName();
    }
    
    public String getChaincodeVersion() {
        if(_chaincodeInvocationSpec == null || _chaincodeInvocationSpec.get() == null) {
            populateChaincodeInvocationSpec();
        }
        return _chaincodeInvocationSpec.get().getChaincodeSpec().getChaincodeId().getVersion();
    }
    
    public String getChaincodePath() {
        if(_chaincodeInvocationSpec == null || _chaincodeInvocationSpec.get() == null) {
            populateChaincodeInvocationSpec();
        }
        return _chaincodeInvocationSpec.get().getChaincodeSpec().getChaincodeId().getPath();
    }
    
    public Array getChaincodeArgs() {
        if(_chaincodeInvocationSpec == null || _chaincodeInvocationSpec.get() == null) {
            populateChaincodeInvocationSpec();
        }
        ChaincodeInput chaincodeInput = _chaincodeInvocationSpec.get().getChaincodeSpec().getInput();
        Object[] args = chaincodeInput.getArgsList().stream().map(arg -> arg.toStringUtf8().replaceAll("[^\\p{Print}]", "??")).toArray();
        return new FabricArray(args);
    }
    
    public int getTimeOut() {
        if(_chaincodeInvocationSpec == null || _chaincodeInvocationSpec.get() == null) {
            populateChaincodeInvocationSpec();
        }
        return _chaincodeInvocationSpec.get().getChaincodeSpec().getTimeout();
    }
    
    public String getRWDataModel() {
        if(_txReadWriteSet == null || _txReadWriteSet.get() == null) {
            populateTxReadWriteSet();
        }
        return _txReadWriteSet.get().getDataModel().name();
    }
    
    public String getResponseMessage() {
        if(_chaincodeAction == null || _chaincodeAction.get() == null) {
            populateChaincodeAction();
        }
        return _chaincodeAction.get().getResponse().getMessage();
    }
    
    public int getResponseStatus() {
        if(_chaincodeAction == null || _chaincodeAction.get() == null) {
            populateChaincodeAction();
        }
        return _chaincodeAction.get().getResponse().getStatus();
    }
    
    public String getResponsePayload() {
        if(_chaincodeAction == null || _chaincodeAction.get() == null) {
            populateChaincodeAction();
        }
        return _chaincodeAction.get().getResponse().getPayload().toStringUtf8();
    }
    
    @SuppressWarnings("unchecked")
    public Array getEndorsements() {
        if(_chaincodeActionPayload == null || _chaincodeActionPayload.get() == null) {
            populateChaincodeActionPayload();
        }
        List<Endorsement> endorsements = _chaincodeActionPayload.get().getAction().getEndorsementsList();
        Object[] endorsementArray = endorsements.stream().map(endorsement -> {
            SerializedIdentity identity;
            try {
                identity = SerializedIdentity.parseFrom(endorsement.getEndorser());
            } catch (Exception e) {
                throw new BlkchnException("Error creating object from ByteString", e);
            }
            JSONObject json = new JSONObject();
            json.put("msp_id", identity.getMspid());
            json.put("signature", identity.getIdBytes().toStringUtf8());
            return json.toJSONString();
        }).toArray();
        return new FabricArray(endorsementArray);
    }
    
    public TxReadWriteSet getTxReadWriteSet() {
        if(_txReadWriteSet == null || _txReadWriteSet.get() == null) {
            populateTxReadWriteSet();
        }
        return _txReadWriteSet.get();
    }
    
    private void populateChaincodeActionPayload() {
        ChaincodeActionPayload chaincodeActionPayload;
        try {
            chaincodeActionPayload = ChaincodeActionPayload.parseFrom(transactionAction.getPayload());
        } catch (InvalidProtocolBufferException e) {
            throw new BlkchnException("Error creating object from ByteString", e);
        }
        _chaincodeActionPayload = new WeakReference<>(chaincodeActionPayload);
    }
    
    private void populateChaincodeInvocationSpec() {
        ChaincodeActionPayload chaincodeActionPayload = null;
        if(_chaincodeActionPayload != null) {
            chaincodeActionPayload = _chaincodeActionPayload.get();
        }
        if(null == chaincodeActionPayload) {
            populateChaincodeActionPayload();
            chaincodeActionPayload = _chaincodeActionPayload.get();
        }
        ChaincodeInvocationSpec chaincodeInvocationSpec;
        try {
            ChaincodeProposalPayload chaincodeProposalPayload = ChaincodeProposalPayload
                    .parseFrom(chaincodeActionPayload.getChaincodeProposalPayload());
            chaincodeInvocationSpec = ChaincodeInvocationSpec.parseFrom(chaincodeProposalPayload.getInput());
        } catch (Exception e) {
            throw new BlkchnException("Error creating object from ByteString", e);
        }
        _chaincodeInvocationSpec = new WeakReference<>(chaincodeInvocationSpec);
    }
    
    private void populateChaincodeAction() {
        ChaincodeActionPayload chaincodeActionPayload = null;
        if(_chaincodeActionPayload != null) {
            chaincodeActionPayload = _chaincodeActionPayload.get();
        }
        if(null == chaincodeActionPayload) {
            populateChaincodeActionPayload();
            chaincodeActionPayload = _chaincodeActionPayload.get();
        }
        ChaincodeEndorsedAction chaincodeEndorsedAction = chaincodeActionPayload.getAction();
        ChaincodeAction chaincodeAction;
        try {
            ProposalResponsePayload proposalResponsePayload = ProposalResponsePayload.parseFrom(chaincodeEndorsedAction.getProposalResponsePayload());
            chaincodeAction = ChaincodeAction.parseFrom(proposalResponsePayload.getExtension());
        } catch (InvalidProtocolBufferException e) {
            throw new BlkchnException("Error creating object from ByteString", e);
        }
        _chaincodeAction = new WeakReference<>(chaincodeAction);
    }
    
    private void populateTxReadWriteSet() {
        ChaincodeAction chaincodeAction = null;
        if(_chaincodeAction != null) {
            chaincodeAction = _chaincodeAction.get();
        }
        if(null == chaincodeAction) {
            populateChaincodeAction();
            chaincodeAction = _chaincodeAction.get();
        }
        TxReadWriteSet txReadWriteSet;
        try {
            txReadWriteSet = TxReadWriteSet.parseFrom(chaincodeAction.getResults());
        } catch (InvalidProtocolBufferException e) {
            throw new BlkchnException("Error creating object from ByteString", e);
        }
        _txReadWriteSet = new WeakReference<>(txReadWriteSet);
    }
}
