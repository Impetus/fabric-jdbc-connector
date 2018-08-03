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
package com.impetus.fabric.parser;

import com.impetus.blkch.sql.parser.LogicalPlan;
import com.impetus.blkch.sql.query.IdentifierNode;
import com.impetus.blkch.sql.user.Affiliation;
import com.impetus.blkch.sql.user.CreateUser;
import com.impetus.blkch.sql.user.Secret;
import com.impetus.fabric.query.QueryBlock;

public class CAManager {

    private LogicalPlan logicalPlan;
    
    private QueryBlock queryBlock;
    
    public CAManager(LogicalPlan logicalPlan, QueryBlock queryBlock) {
        this.logicalPlan = logicalPlan;
        this.queryBlock = queryBlock;
    }
    
    public void registerUser() {
        CreateUser createUser = logicalPlan.getCreateUser();
        String username = createUser.getChildType(IdentifierNode.class, 0).getValue();
        String secret = createUser.getChildType(Secret.class, 0).getValue();
        String affiliation = createUser.getChildType(Affiliation.class, 0).getValue();
        queryBlock.registerUser(username, secret, affiliation);
    }
}
