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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.impetus.blkch.BlkchnException;
import com.impetus.blkch.sql.DataFrame;
import com.impetus.blkch.sql.insert.ColumnValue;
import com.impetus.blkch.sql.parser.LogicalPlan;
import com.impetus.blkch.sql.parser.TreeNode;
import com.impetus.blkch.sql.query.IdentifierNode;
import com.impetus.blkch.sql.query.Table;
import com.impetus.blkch.util.Utilities;
import com.impetus.fabric.query.QueryBlock;

public class InsertExecutor {

    private LogicalPlan logicalPlan;
    
    private QueryBlock queryBlock;
    
    public InsertExecutor(LogicalPlan logicalPlan, QueryBlock queryBlock) {
        this.logicalPlan = logicalPlan;
        this.queryBlock = queryBlock;
    }
    
    public DataFrame executeInsert() {
        TreeNode insert = logicalPlan.getInsert();
        String chaincodeName = insert.getChildType(Table.class, 0).getChildType(IdentifierNode.class, 0).getValue();
        List<String> args = new ArrayList<>();
        List<IdentifierNode> idents = insert.getChildType(ColumnValue.class, 0).getChildType(IdentifierNode.class);
        if(idents.size() == 0) {
            throw new BlkchnException("Invalid number of parameters");
        }
        for(IdentifierNode ident : idents) {
            args.add(Utilities.unquote(ident.getValue()));
        }
        return queryBlock.invokeChaincode(chaincodeName, args.get(0), args.stream().skip(1).collect(Collectors.toList()).toArray(new String[]{}));
    }
}
