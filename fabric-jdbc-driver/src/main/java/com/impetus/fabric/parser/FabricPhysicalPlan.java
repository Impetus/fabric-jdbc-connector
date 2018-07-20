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

import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
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
    
    private static Map<String, Map<String, Integer>> fabricTableColumnTypeMap = new HashMap<>();

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
        
        fabricTableColumnTypeMap.put(FabricTables.BLOCK, 
                ImmutableMap.<String, Integer>builder()
                .put(FabricColumns.PREVIOUS_HASH, Types.VARCHAR)
                .put(FabricColumns.BLOCK_DATA_HASH, Types.VARCHAR)
                .put(FabricColumns.TRANS_ACTIONS_META_DATA, Types.VARCHAR)
                .put(FabricColumns.TRANSACTION_COUNT, Types.INTEGER)
                .put(FabricColumns.BLOCK_NO, Types.BIGINT)
                .put(FabricColumns.CHANNEL_ID, Types.VARCHAR)
                .build());
        
        fabricTableColumnTypeMap.put(FabricTables.TRANSACTION,  
                ImmutableMap.<String, Integer>builder()
                .put(FabricColumns.BLOCK_NO, Types.BIGINT)
                .put(FabricColumns.TRANSACTION_ID, Types.VARCHAR)
                .put(FabricColumns.HEADER_TYPE, Types.INTEGER)
                .put(FabricColumns.MESSAGE_PROTOCOL_VERSION, Types.INTEGER)
                .put(FabricColumns.TIMESTAMP, Types.JAVA_OBJECT)
                .put(FabricColumns.EPOCH, Types.BIGINT)
                .put(FabricColumns.CHANNEL_ID, Types.VARCHAR)
                .put(FabricColumns.CREATOR_MSP, Types.VARCHAR)
                .put(FabricColumns.CREATOR_SIGNATURE, Types.VARCHAR)
                .put(FabricColumns.NONCE, Types.VARCHAR)
                .build());
        
        fabricTableColumnTypeMap.put(FabricTables.TRANSACTION_ACTION, 
                ImmutableMap.<String, Integer>builder()
                .put(FabricColumns.BLOCK_NO, Types.BIGINT)
                .put(FabricColumns.TRANSACTION_ID, Types.VARCHAR)
                .put(FabricColumns.ID_GENERATION_ALG, Types.VARCHAR)
                .put(FabricColumns.CHAINCODE_TYPE, Types.VARCHAR)
                .put(FabricColumns.CHAINCODE_NAME, Types.VARCHAR)
                .put(FabricColumns.CHAINCODE_VERSION, Types.VARCHAR)
                .put(FabricColumns.CHAINCODE_PATH, Types.VARCHAR)
                .put(FabricColumns.CHAINCODE_ARGS, Types.ARRAY)
                .put(FabricColumns.TIME_OUT, Types.INTEGER)
                .put(FabricColumns.RW_DATAMODEL, Types.VARCHAR)
                .put(FabricColumns.RESPONSE_MESSAGE, Types.VARCHAR)
                .put(FabricColumns.RESPONSE_STATUS, Types.INTEGER)
                .put(FabricColumns.RESPONSE_PAYLOAD, Types.VARCHAR)
                .put(FabricColumns.ENDORSEMENTS, Types.ARRAY)
                .build());
        
        fabricTableColumnTypeMap.put(FabricTables.READ_WRITE_SET,
                ImmutableMap.<String, Integer>builder()
                .put(FabricColumns.BLOCK_NO, Types.BIGINT)
                .put(FabricColumns.TRANSACTION_ID, Types.VARCHAR)
                .put(FabricColumns.NAMESPACE, Types.VARCHAR)
                .put(FabricColumns.READ_KEY, Types.VARCHAR)
                .put(FabricColumns.READ_BLOCK_NO, Types.BIGINT)
                .put(FabricColumns.READ_TX_NUM, Types.BIGINT)
                .put(FabricColumns.RANGE_QUERY_START_KEY, Types.VARCHAR)
                .put(FabricColumns.RANGE_QUERY_END_KEY, Types.VARCHAR)
                .put(FabricColumns.RANGE_QUERY_ITR_EXAUSTED, Types.BOOLEAN)
                .put(FabricColumns.RANGE_QUERY_READS_INFO, Types.VARCHAR)
                .put(FabricColumns.WRITE_KEY, Types.VARCHAR)
                .put(FabricColumns.IS_DELETE, Types.BOOLEAN)
                .put(FabricColumns.WRITE_VALUE, Types.VARCHAR)
                .build());
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

    @Override
    public Map<String, Integer> getColumnTypeMap(String table) {
        return fabricTableColumnTypeMap.get(table);
    }
    
    public static Map<String, Integer> getColumnTypes(String table) {
        return fabricTableColumnTypeMap.get(table);
    }

}
