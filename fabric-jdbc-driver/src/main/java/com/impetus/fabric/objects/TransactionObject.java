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
