package com.impetus.fabric.parser;

import com.impetus.blkch.sql.parser.LogicalPlan;
import com.impetus.fabric.query.QueryBlock;

public class CAManager {

    private LogicalPlan logicalPlan;
    
    private QueryBlock queryBlock;
    
    public CAManager(LogicalPlan logicalPlan, QueryBlock queryBlock) {
        this.logicalPlan = logicalPlan;
        this.queryBlock = queryBlock;
    }
    
    public void registerUser() {
        
    }
}
