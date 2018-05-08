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

    private static List<String> fabricTables = Arrays.asList(FabricTables.BLOCK, FabricTables.TRANSACTION,
            FabricTables.TRANSACTION_ACTION, FabricTables.READ_WRITE_SET);

    private static Map<String, List<String>> fabricTableColumnMap = new HashMap<>();

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
        rangeOpMap
                .put(new Tuple2<>(FabricTables.TRANSACTION_ACTION, FabricColumns.BLOCK_NO), new LongRangeOperations());
        rangeOpMap.put(new Tuple2<>(FabricTables.READ_WRITE_SET, FabricColumns.BLOCK_NO), new LongRangeOperations());

        fabricTableColumnMap.put(FabricTables.BLOCK, Arrays.asList(FabricColumns.PREVIOUS_HASH,
                FabricColumns.BLOCK_DATA_HASH, FabricColumns.TRANS_ACTIONS_META_DATA, FabricColumns.TRANSACTION_COUNT,
                FabricColumns.BLOCK_NO, FabricColumns.CHANNEL_ID));
        fabricTableColumnMap.put(FabricTables.TRANSACTION, Arrays.asList(FabricColumns.BLOCK_NO,
                FabricColumns.TRANSACTION_ID, FabricColumns.HEADER_TYPE, FabricColumns.MESSAGE_PROTOCOL_VERSION,
                FabricColumns.TIMESTAMP, FabricColumns.EPOCH, FabricColumns.CHANNEL_ID, FabricColumns.CREATOR_MSP,
                FabricColumns.CREATOR_SIGNATURE, FabricColumns.NONCE));
        fabricTableColumnMap.put(FabricTables.TRANSACTION_ACTION, Arrays.asList(FabricColumns.BLOCK_NO,
                FabricColumns.TRANSACTION_ID, FabricColumns.ID_GENERATION_ALG, FabricColumns.CHAINCODE_TYPE,
                FabricColumns.CHAINCODE_NAME, FabricColumns.CHAINCODE_VERSION, FabricColumns.CHAINCODE_PATH,
                FabricColumns.CHAINCODE_ARGS, FabricColumns.TIME_OUT, FabricColumns.RW_DATAMODEL,
                FabricColumns.RESPONSE_MESSAGE, FabricColumns.RESPONSE_STATUS, FabricColumns.RESPONSE_PAYLOAD,
                FabricColumns.ENDORSEMENTS));
        fabricTableColumnMap.put(FabricTables.READ_WRITE_SET, Arrays.asList(FabricColumns.BLOCK_NO,
                FabricColumns.TRANSACTION_ID, FabricColumns.NAMESPACE, FabricColumns.READ_KEY,
                FabricColumns.READ_BLOCK_NO, FabricColumns.READ_TX_NUM, FabricColumns.RANGE_QUERY_START_KEY,
                FabricColumns.RANGE_QUERY_END_KEY, FabricColumns.RANGE_QUERY_ITR_EXAUSTED,
                FabricColumns.RANGE_QUERY_READS_INFO, FabricColumns.WRITE_KEY, FabricColumns.IS_DELETE,
                FabricColumns.WRITE_VALUE));
    }

    public FabricPhysicalPlan(LogicalPlan logicalPlan) {
        super(DESCRIPTION, logicalPlan);
    }

    @Override
    public List<String> getRangeCols(String table) {
        if (!rangeColMap.containsKey(table)) {
            return new ArrayList<>();
        }
        return rangeColMap.get(table);
    }

    @Override
    public List<String> getQueryCols(String table) {
        if (!queryColMap.containsKey(table)) {
            return new ArrayList<>();
        }
        return queryColMap.get(table);
    }

    @Override
    public RangeOperations<?> getRangeOperations(String table, String column) {
        if (!rangeOpMap.containsKey(new Tuple2<>(table, column))) {
            throw new BlkchnException(String.format("Column %s of table %s can't be queried by range", table, column));
        }
        return rangeOpMap.get(new Tuple2<>(table, column));
    }

    @Override
    public boolean tableExists(String table) {
        return fabricTables.contains(table);
    }

    @Override
    public boolean columnExists(String table, String column) {
        if (!fabricTableColumnMap.containsKey(table)) {
            return false;
        }
        return fabricTableColumnMap.get(table).contains(column);
    }

    static Map<String, List<String>> getFabricTableColumnMap() {
        return fabricTableColumnMap;
    }

}
