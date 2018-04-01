package com.impetus.fabric.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.impetus.blkch.BlkchnException;
import com.impetus.blkch.sql.parser.LogicalPlan;
import com.impetus.blkch.sql.parser.PhysicalPlan;
import com.impetus.blkch.util.LongRangeOperations;
import com.impetus.blkch.util.RangeOperations;
import com.impetus.blkch.util.Tuple2;
import com.impetus.fabric.query.FabricColumns;
import com.impetus.fabric.query.FabricTables;

public class FabricPhysicalPlan extends PhysicalPlan {
    
    public static final String DESCRIPTION = "FABRIC_PHYSICAL_PLAN";
    
    private static Map<String, List<String>> rangeColMap = new HashMap<>();
    
    private static Map<String, List<String>> queryColMap = new HashMap<>();
    
    private static Map<Tuple2<String, String>, RangeOperations<?>> rangeOpMap = new HashMap<>();
    
    static {
        rangeColMap.put(FabricTables.BLOCK, Arrays.asList(FabricColumns.BLOCK_NO));
        queryColMap.put(FabricTables.BLOCK, Arrays.asList(FabricColumns.PREVIOUS_HASH));
        
        rangeColMap.put(FabricTables.TRANSACTION, Arrays.asList(FabricColumns.BLOCK_NO));
        queryColMap.put(FabricTables.TRANSACTION, Arrays.asList(FabricColumns.TRANSACTION_ID));
        
        rangeColMap.put(FabricTables.TRANSACTION_ACTION, Arrays.asList(FabricColumns.BLOCK_NO));
        queryColMap.put(FabricTables.TRANSACTION_ACTION, Arrays.asList(FabricColumns.TRANSACTION_ID));
        
        rangeColMap.put(FabricTables.READ_WRITE_SET, Arrays.asList(FabricColumns.BLOCK_NO));
        queryColMap.put(FabricTables.READ_WRITE_SET, Arrays.asList(FabricColumns.TRANSACTION_ID));
        
        rangeOpMap.put(new Tuple2<>(FabricTables.BLOCK, FabricColumns.BLOCK_NO), new LongRangeOperations());
        rangeOpMap.put(new Tuple2<>(FabricTables.TRANSACTION, FabricColumns.BLOCK_NO), new LongRangeOperations());
        rangeOpMap.put(new Tuple2<>(FabricTables.TRANSACTION_ACTION, FabricColumns.BLOCK_NO), new LongRangeOperations());
        rangeOpMap.put(new Tuple2<>(FabricTables.READ_WRITE_SET, FabricColumns.BLOCK_NO), new LongRangeOperations());
    }

    public FabricPhysicalPlan(LogicalPlan logicalPlan) {
        super(DESCRIPTION, logicalPlan);
    }

    @Override
    public List<String> getRangeCols(String table) {
        if(!rangeColMap.containsKey(table)) {
            return new ArrayList<>();
        }
        return rangeColMap.get(table);
    }

    @Override
    public List<String> getQueryCols(String table) {
        if(!queryColMap.containsKey(table)) {
            return new ArrayList<>();
        }
        return queryColMap.get(table);
    }

    @Override
    public RangeOperations<?> getRangeOperations(String table, String column) {
        if(!rangeOpMap.containsKey(new Tuple2<>(table, column))) {
            throw new BlkchnException(String.format("Column %s of table %s can't be queried by range", table, column));
        }
        return rangeOpMap.get(new Tuple2<>(table, column));
    }

}
