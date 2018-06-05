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
