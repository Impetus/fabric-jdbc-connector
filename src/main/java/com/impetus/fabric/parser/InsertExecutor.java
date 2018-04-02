package com.impetus.fabric.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.impetus.blkch.BlkchnException;
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
    
    public void executeInsert() {
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
        queryBlock.invokeChaincode(chaincodeName, args.get(0), args.stream().skip(1).collect(Collectors.toList()).toArray(new String[]{}));
    }
}
