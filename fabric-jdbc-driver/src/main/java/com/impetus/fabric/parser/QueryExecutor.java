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

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.hyperledger.fabric.protos.ledger.rwset.Rwset.NsReadWriteSet;
import org.hyperledger.fabric.protos.ledger.rwset.Rwset.TxReadWriteSet;
import org.hyperledger.fabric.protos.ledger.rwset.kvrwset.KvRwset.KVRWSet;
import org.hyperledger.fabric.protos.ledger.rwset.kvrwset.KvRwset.KVRead;
import org.hyperledger.fabric.protos.ledger.rwset.kvrwset.KvRwset.KVWrite;
import org.hyperledger.fabric.protos.ledger.rwset.kvrwset.KvRwset.RangeQueryInfo;
import org.hyperledger.fabric.protos.peer.FabricTransaction.TransactionAction;
import org.hyperledger.fabric.sdk.BlockInfo;
import org.hyperledger.fabric.sdk.BlockInfo.EnvelopeInfo;
import org.hyperledger.fabric.sdk.Channel;
import org.hyperledger.fabric.sdk.TransactionInfo;
import org.hyperledger.fabric.sdk.exception.InvalidArgumentException;
import org.hyperledger.fabric.sdk.exception.ProposalException;

import com.google.protobuf.InvalidProtocolBufferException;
import com.impetus.blkch.BlkchnException;
import com.impetus.blkch.sql.DataFrame;
import com.impetus.blkch.sql.GroupedDataFrame;
import com.impetus.blkch.sql.parser.AbstractQueryExecutor;
import com.impetus.blkch.sql.parser.LogicalPlan;
import com.impetus.blkch.sql.parser.TreeNode;
import com.impetus.blkch.sql.query.Column;
import com.impetus.blkch.sql.query.Comparator;
import com.impetus.blkch.sql.query.DataNode;
import com.impetus.blkch.sql.query.DirectAPINode;
import com.impetus.blkch.sql.query.FromItem;
import com.impetus.blkch.sql.query.GroupByClause;
import com.impetus.blkch.sql.query.HavingClause;
import com.impetus.blkch.sql.query.IdentifierNode;
import com.impetus.blkch.sql.query.LimitClause;
import com.impetus.blkch.sql.query.LogicalOperation;
import com.impetus.blkch.sql.query.LogicalOperation.Operator;
import com.impetus.blkch.sql.query.OrderByClause;
import com.impetus.blkch.sql.query.OrderItem;
import com.impetus.blkch.sql.query.RangeNode;
import com.impetus.blkch.sql.query.Table;
import com.impetus.blkch.util.Range;
import com.impetus.blkch.util.RangeOperations;
import com.impetus.blkch.util.Tuple2;
import com.impetus.blkch.util.Tuple3;
import com.impetus.fabric.objects.ReadWriteSetDeserializer;
import com.impetus.fabric.objects.ReadWriteSetObject;
import com.impetus.fabric.objects.TransactionActionDeserializer;
import com.impetus.fabric.objects.TransactionActionObject;
import com.impetus.fabric.objects.TransactionDeserializer;
import com.impetus.fabric.objects.TransactionObject;
import com.impetus.fabric.query.FabricColumns;
import com.impetus.fabric.query.FabricTables;
import com.impetus.fabric.query.QueryBlock;

public class QueryExecutor extends AbstractQueryExecutor {

    private QueryBlock queryBlock;
    
    public QueryExecutor(LogicalPlan logicalPlan, QueryBlock queryBlock) {
        this.logicalPlan = logicalPlan;
        this.queryBlock = queryBlock;
        this.physicalPlan = new FabricPhysicalPlan(logicalPlan);
    }

    public DataFrame executeQuery() {
        physicalPlan.getWhereClause().traverse();
        if (!physicalPlan.validateLogicalPlan()) {
            throw new BlkchnException("This query can't be executed as it requires fetching huge amount of data");
        }
        DataFrame dataframe = getFromTable();
        if(dataframe.isEmpty()) {
            return dataframe;
        }
        List<OrderItem> orderItems = null;
        if (logicalPlan.getQuery().hasChildType(OrderByClause.class)) {
            OrderByClause orderByClause = logicalPlan.getQuery().getChildType(OrderByClause.class, 0);
            orderItems = orderByClause.getChildType(OrderItem.class);
        }
        LimitClause limitClause = null;
        if (logicalPlan.getQuery().hasChildType(LimitClause.class)) {
            limitClause = logicalPlan.getQuery().getChildType(LimitClause.class, 0);
        }
        if (logicalPlan.getQuery().hasChildType(GroupByClause.class)) {
            GroupByClause groupByClause = logicalPlan.getQuery().getChildType(GroupByClause.class, 0);
            List<Column> groupColumns = groupByClause.getChildType(Column.class);
            List<String> groupByCols = groupColumns.stream()
                    .map(col -> col.getChildType(IdentifierNode.class, 0).getValue()).collect(Collectors.toList());
            GroupedDataFrame groupedDF = dataframe.group(groupByCols);
            DataFrame afterSelect;
            if(logicalPlan.getQuery().hasChildType(HavingClause.class)) {
                afterSelect = groupedDF.having(logicalPlan.getQuery().getChildType(HavingClause.class, 0)).select(physicalPlan.getSelectItems());
            } else {
                afterSelect = groupedDF.select(physicalPlan.getSelectItems());
            }
            DataFrame afterOrder;
            if (orderItems != null) {
                afterOrder = afterSelect.order(orderItems);
            } else {
                afterOrder = afterSelect;
            }
            if (limitClause == null) {
                return afterOrder;
            } else {
                return afterOrder.limit(limitClause);
            }
        }
        DataFrame preSelect;
        if (orderItems != null) {
            preSelect = dataframe.order(orderItems);
        } else {
            preSelect = dataframe;
        }
        DataFrame afterOrder;
        if (limitClause == null) {
            afterOrder = preSelect;
        } else {
            afterOrder = preSelect.limit(limitClause);
        }
        return afterOrder.select(physicalPlan.getSelectItems());
    }

    private DataFrame getFromTable() {
        Table table = logicalPlan.getQuery().getChildType(FromItem.class, 0).getChildType(Table.class, 0);
        String tableName = table.getChildType(IdentifierNode.class, 0).getValue();
        if (physicalPlan.getWhereClause() != null) {
            DataNode<?> finalData;
            if (physicalPlan.getWhereClause().hasChildType(LogicalOperation.class)) {
                TreeNode directAPIOptimizedTree = executeDirectAPIs(tableName, physicalPlan.getWhereClause()
                        .getChildType(LogicalOperation.class, 0));
                TreeNode optimizedTree = optimize(directAPIOptimizedTree);
                 finalData = execute(optimizedTree);
            } else if (physicalPlan.getWhereClause().hasChildType(DirectAPINode.class)) {
                DirectAPINode node = physicalPlan.getWhereClause().getChildType(DirectAPINode.class, 0);
                finalData = getDataNode(node.getTable(), node.getColumn(), node.getValue());
            } else {
                RangeNode<?> rangeNode = physicalPlan.getWhereClause().getChildType(RangeNode.class, 0);
                finalData = executeRangeNode(rangeNode);
                finalData.traverse();
            }
            return createDataFrame(finalData);
        } else {
            throw new BlkchnException("Can't query without where clause. Data will be huge");
        }

    }

    protected DataNode<?> getDataNode(String table, String column, String value) {
        if (dataMap.containsKey(value)) {
            return new DataNode<>(table, Arrays.asList(value));
        }
        Channel channel = queryBlock.getChannel();
        if (table.equals(FabricTables.BLOCK) && column.equals(FabricColumns.BLOCK_NO)) {
            try {
                BlockInfo blockInfo = channel.queryBlockByNumber(Long.parseLong(value));
                dataMap.put(Long.toString(blockInfo.getBlockNumber()), blockInfo);
                return new DataNode<>(table, Arrays.asList(Long.toString(blockInfo.getBlockNumber())));
            } catch (NumberFormatException | InvalidArgumentException | ProposalException e) {
                throw new BlkchnException("Error querying block by block number " + value, e);
            }
        } else if (table.equals(FabricTables.BLOCK) && column.equals(FabricColumns.PREVIOUS_HASH)) {
            try {
                BlockInfo blockInfo = channel.queryBlockByHash(Hex.decodeHex(value.replace("'", "").toCharArray()));
                dataMap.put(Long.toString(blockInfo.getBlockNumber()), blockInfo);
                return new DataNode<>(table, Arrays.asList(Long.toString(blockInfo.getBlockNumber())));
            } catch (InvalidArgumentException | ProposalException | DecoderException e) {
                throw new BlkchnException("Error querying block by hash " + value.replace("'", ""), e);
            }
        } else if(FabricTables.TRANSACTION.equals(table) && FabricColumns.TRANSACTION_ID.equals(column)) {
            try {
                long blockNo = channel.queryBlockByTransactionID(value.replace("'", "")).getBlockNumber();
                TransactionInfo transactionInfo = channel.queryTransactionByID(value.replace("'", ""));
                dataMap.put(value.replace("'", ""), new TransactionObject(blockNo, transactionInfo));
                return new DataNode<>(table, Arrays.asList(value.replace("'", "")));
            } catch (ProposalException | InvalidArgumentException e) {
                throw new BlkchnException("Error querying transaction by id " + value.replace("'", ""), e);
            }
        } else if(FabricTables.TRANSACTION.equals(table) && FabricColumns.BLOCK_NO.equals(column)) {
            try {
                BlockInfo blockInfo = channel.queryBlockByNumber(Long.parseLong(value));
                long blockNo = blockInfo.getBlockNumber();
                List<String> transactions = new ArrayList<>();
                for(EnvelopeInfo envelopeInfo : blockInfo.getEnvelopeInfos()) {
                    String transactionId = envelopeInfo.getTransactionID();
                    TransactionInfo transactionInfo = channel.queryTransactionByID(transactionId);
                    dataMap.put(transactionId, new TransactionObject(blockNo, transactionInfo));
                    transactions.add(transactionId);
                }
                return new DataNode<>(table, transactions);
            } catch (NumberFormatException | InvalidArgumentException | ProposalException e) {
                throw new BlkchnException("Error querying transactions for block number " + value, e);
            }
        } else if(FabricTables.TRANSACTION_ACTION.equals(table) && FabricColumns.TRANSACTION_ID.equals(column)) {
            try {
                long blockNo = channel.queryBlockByTransactionID(value.replace("'", "")).getBlockNumber();
                TransactionInfo transactionInfo = channel.queryTransactionByID(value.replace("'", ""));
                TransactionObject transactionObject = new TransactionObject(blockNo, transactionInfo);
                List<TransactionAction> transactionActions = transactionObject.getDeserializer().getTransactionActions();
                List<String> actions = new ArrayList<>();
                for(int i = 0 ; i < transactionActions.size() ; i++) {
                    dataMap.put(Integer.toString(new Tuple2<>(transactionInfo.getTransactionID(), i).hashCode()), new TransactionActionObject(blockNo, transactionInfo.getTransactionID(),
                            transactionActions.get(i)));
                    actions.add(Integer.toString(new Tuple2<>(transactionInfo.getTransactionID(), i).hashCode()));
                }
                return new DataNode<>(table, actions);
            } catch (ProposalException | InvalidArgumentException e) {
                throw new BlkchnException("Error querying transaction actions by id " + value.replace("'", ""), e);
            }
        } else if(FabricTables.TRANSACTION_ACTION.equals(table) && FabricColumns.BLOCK_NO.equals(column)) {
            try {
                BlockInfo blockInfo = channel.queryBlockByNumber(Long.parseLong(value));
                List<String> actions = new ArrayList<>();
                for(EnvelopeInfo envelopeInfo : blockInfo.getEnvelopeInfos()) {
                    String transactionId = envelopeInfo.getTransactionID();
                    TransactionInfo transactionInfo = channel.queryTransactionByID(transactionId);
                    TransactionObject transactionObject = new TransactionObject(blockInfo.getBlockNumber(), transactionInfo);
                    List<TransactionAction> transactionActions = transactionObject.getDeserializer().getTransactionActions();
                    for(int i = 0 ; i < transactionActions.size() ; i++) {
                        dataMap.put(Integer.toString(new Tuple2<>(transactionInfo.getTransactionID(), i).hashCode()), new TransactionActionObject(blockInfo.getBlockNumber(), 
                                transactionInfo.getTransactionID(), transactionActions.get(i)));
                        actions.add(Integer.toString(new Tuple2<>(transactionInfo.getTransactionID(), i).hashCode()));
                    }
                }
                return new DataNode<>(table, actions);
            } catch (NumberFormatException | InvalidArgumentException | ProposalException e) {
                throw new BlkchnException("Error querying transaction actions for block number " + value, e);
            }
        } else if(FabricTables.READ_WRITE_SET.equals(table) && FabricColumns.TRANSACTION_ID.equals(column)) {
            try {
                long blockNo = channel.queryBlockByTransactionID(value.replace("'", "")).getBlockNumber();
                TransactionInfo transactionInfo = channel.queryTransactionByID(value.replace("'", ""));
                String transactionId = transactionInfo.getTransactionID();
                TransactionObject transactionObject = new TransactionObject(blockNo, transactionInfo);
                List<TransactionAction> transactionActions = transactionObject.getDeserializer().getTransactionActions();
                List<String> keys = new ArrayList<>();
                for(int i = 0 ; i < transactionActions.size() ; i++) {
                    TxReadWriteSet readWriteSet = new TransactionActionDeserializer(transactionActions.get(i)).getTxReadWriteSet();
                    for(int j = 0 ; j < readWriteSet.getNsRwsetCount() ; j++) {
                        NsReadWriteSet ns = readWriteSet.getNsRwset(j);
                        String namespace = ns.getNamespace();
                        KVRWSet set = KVRWSet.parseFrom(ns.getRwset());
                        for(int k = 0 ; k < set.getReadsCount() ; k++) {
                            KVRead read = set.getReads(k);
                            String key = Integer.toString(new Tuple2<>(new Tuple2<>(transactionId, i), new Tuple3<>(j, "read", k)).hashCode());
                            dataMap.put(key, new ReadWriteSetObject(blockNo, transactionId, namespace, read));
                            keys.add(key);
                        }
                        for(int k = 0 ; k < set.getRangeQueriesInfoCount() ; k++) {
                            RangeQueryInfo rangeQueryInfo = set.getRangeQueriesInfo(k);
                            String key = Integer.toString(new Tuple2<>(new Tuple2<>(transactionId, i), new Tuple3<>(j, "rangeQueryInfo", k)).hashCode());
                            dataMap.put(key, new ReadWriteSetObject(blockNo, transactionId, namespace, rangeQueryInfo));
                            keys.add(key);
                        }
                        for(int k = 0 ; k < set.getWritesCount() ; k++) {
                            KVWrite write = set.getWrites(k);
                            String key = Integer.toString(new Tuple2<>(new Tuple2<>(transactionId, i), new Tuple3<>(j, "write", k)).hashCode());
                            dataMap.put(key, new ReadWriteSetObject(blockNo, transactionId, namespace, write));
                            keys.add(key);
                        }
                    }
                }
                return new DataNode<>(table, keys);
            } catch (InvalidArgumentException | ProposalException | InvalidProtocolBufferException e) {
                throw new BlkchnException("Error querying read write sets by id " + value.replace("'", ""), e);
            }
        } else if(FabricTables.READ_WRITE_SET.equals(table) && FabricColumns.BLOCK_NO.equals(column)) {
            try {
                BlockInfo blockInfo = channel.queryBlockByNumber(Long.parseLong(value));
                long blockNo = blockInfo.getBlockNumber();
                List<String> keys = new ArrayList<>();
                for(EnvelopeInfo envelopeInfo : blockInfo.getEnvelopeInfos()) {
                    String transactionId = envelopeInfo.getTransactionID();
                    TransactionInfo transactionInfo = channel.queryTransactionByID(transactionId);
                    TransactionObject transactionObject = new TransactionObject(blockInfo.getBlockNumber(), transactionInfo);
                    List<TransactionAction> transactionActions = transactionObject.getDeserializer().getTransactionActions();
                    for(int i = 0 ; i < transactionActions.size() ; i++) {
                        TxReadWriteSet readWriteSet = new TransactionActionDeserializer(transactionActions.get(i)).getTxReadWriteSet();
                        for(int j = 0 ; j < readWriteSet.getNsRwsetCount() ; j++) {
                            NsReadWriteSet ns = readWriteSet.getNsRwset(j);
                            String namespace = ns.getNamespace();
                            KVRWSet set = KVRWSet.parseFrom(ns.getRwset());
                            for(int k = 0 ; k < set.getReadsCount() ; k++) {
                                KVRead read = set.getReads(k);
                                String key = Integer.toString(new Tuple2<>(new Tuple2<>(transactionId, i), new Tuple3<>(j, "read", k)).hashCode());
                                dataMap.put(key, new ReadWriteSetObject(blockNo, transactionId, namespace, read));
                                keys.add(key);
                            }
                            for(int k = 0 ; k < set.getRangeQueriesInfoCount() ; k++) {
                                RangeQueryInfo rangeQueryInfo = set.getRangeQueriesInfo(k);
                                String key = Integer.toString(new Tuple2<>(new Tuple2<>(transactionId, i), new Tuple3<>(j, "rangeQueryInfo", k)).hashCode());
                                dataMap.put(key, new ReadWriteSetObject(blockNo, transactionId, namespace, rangeQueryInfo));
                                keys.add(key);
                            }
                            for(int k = 0 ; k < set.getWritesCount() ; k++) {
                                KVWrite write = set.getWrites(k);
                                String key = Integer.toString(new Tuple2<>(new Tuple2<>(transactionId, i), new Tuple3<>(j, "write", k)).hashCode());
                                dataMap.put(key, new ReadWriteSetObject(blockNo, transactionId, namespace, write));
                                keys.add(key);
                            }
                        }
                    }
                }
                return new DataNode<>(table, keys);
            } catch (NumberFormatException | InvalidArgumentException | ProposalException | InvalidProtocolBufferException e) {
                throw new BlkchnException("Error querying read write sets for block number " + value, e);
            }
        } else {
            throw new BlkchnException(String.format("There is no direct API for table %s and column %s combination",
                    table, column));
        }
    }

    @SuppressWarnings("unchecked")
    protected <T extends Number & Comparable<T>> DataNode<?> executeRangeNode(RangeNode<T> rangeNode) {
        if(rangeNode.getRangeList().getRanges().isEmpty()) {
            return new DataNode<>(rangeNode.getTable(), new ArrayList<>());
        }
        RangeOperations<T> rangeOps = (RangeOperations<T>) physicalPlan.getRangeOperations(rangeNode.getTable(),
                rangeNode.getColumn());
        String rangeCol = rangeNode.getColumn();
        String rangeTable = rangeNode.getTable();
        Channel channel = queryBlock.getChannel();
        Long height;
        try {
            height = channel.queryBlockchainInfo().getHeight();
        } catch (ProposalException | InvalidArgumentException e) {
            throw new BlkchnException("Error getting height of ledger", e);
        }
        List<DataNode<String>> dataNodes = rangeNode.getRangeList().getRanges().stream().map(range -> {
            List<String> keys = new ArrayList<>();
            T current = range.getMin().equals(rangeOps.getMinValue()) ? (T) new Long(0l) : range.getMin();
            T max = range.getMax().equals(rangeOps.getMaxValue()) ? (T) (new Long(height -1)) : range.getMax();
            do {
                if (FabricTables.BLOCK.equals(rangeTable) && FabricColumns.BLOCK_NO.equals(rangeCol)) {
                    if(Long.parseLong(current.toString()) >= height || Long.parseLong(current.toString()) <= 0l) {
                        current = rangeOps.add(current, 1);
                        continue;
                    }
                    try {
                        if (dataMap.get(current.toString()) != null) {
                            keys.add(current.toString());
                        } else {
                            BlockInfo blockInfo = channel.queryBlockByNumber(Long.parseLong(current.toString()));
                            dataMap.put(Long.toString(blockInfo.getBlockNumber()), blockInfo);
                            keys.add(current.toString());
                        }
                    } catch (Exception e) {
                        throw new BlkchnException("Error query block by number " + current, e);
                    }
                } else if(FabricTables.TRANSACTION.equals(rangeTable) && FabricColumns.BLOCK_NO.equals(rangeCol)) {
                    if(Long.parseLong(current.toString()) >= height || Long.parseLong(current.toString()) <= 0l) {
                        current = rangeOps.add(current, 1);
                        continue;
                    }
                    try {
                        BlockInfo blockInfo = channel.queryBlockByNumber(Long.parseLong(current.toString()));
                        long blockNum = blockInfo.getBlockNumber();
                        for(EnvelopeInfo envelopeInfo : blockInfo.getEnvelopeInfos()) {
                            if(dataMap.get(envelopeInfo.getTransactionID()) != null) {
                                keys.add(envelopeInfo.getTransactionID());
                            } else {
                                TransactionInfo transactionInfo = channel.queryTransactionByID(envelopeInfo.getTransactionID());
                                dataMap.put(envelopeInfo.getTransactionID(), new TransactionObject(blockNum, transactionInfo));
                                keys.add(envelopeInfo.getTransactionID());
                            }
                        }
                    } catch (Exception e) {
                        throw new BlkchnException("Error query block by number " + current, e);
                    }
                } else if(FabricTables.TRANSACTION_ACTION.equals(rangeTable) && FabricColumns.BLOCK_NO.equals(rangeCol)) {
                    if(Long.parseLong(current.toString()) >= height || Long.parseLong(current.toString()) <= 0l) {
                        current = rangeOps.add(current, 1);
                        continue;
                    }
                    try {
                        BlockInfo blockInfo = channel.queryBlockByNumber(Long.parseLong(current.toString()));
                        long blockNum = blockInfo.getBlockNumber();
                        for(EnvelopeInfo envelopeInfo : blockInfo.getEnvelopeInfos()) {
                            String transactionId = envelopeInfo.getTransactionID();
                            TransactionInfo transactionInfo = channel.queryTransactionByID(transactionId);
                            TransactionObject transactionObject = new TransactionObject(blockNum, transactionInfo);
                            List<TransactionAction> transactionActions = transactionObject.getDeserializer().getTransactionActions();
                            for(int i = 0 ; i < transactionActions.size() ; i++) {
                                if(dataMap.get(Integer.toString(new Tuple2<>(transactionId, i).hashCode())) != null) {
                                    keys.add(Integer.toString(new Tuple2<>(transactionId, i).hashCode()));
                                } else {
                                    TransactionActionObject transactionActionObject = new TransactionActionObject(blockNum, transactionId, transactionActions.get(i));
                                    dataMap.put(Integer.toString(new Tuple2<>(transactionId, i).hashCode()), transactionActionObject);
                                    keys.add(Integer.toString(new Tuple2<>(transactionId, i).hashCode()));
                                }
                            }
                        }
                    } catch (Exception e) {
                        throw new BlkchnException("Error query block by number " + current, e);
                    }
                } else if(FabricTables.READ_WRITE_SET.equals(rangeTable) && FabricColumns.BLOCK_NO.equals(rangeCol)) {
                    if(Long.parseLong(current.toString()) >= height || Long.parseLong(current.toString()) <= 0l) {
                        current = rangeOps.add(current, 1);
                        continue;
                    }
                    try {
                        BlockInfo blockInfo = channel.queryBlockByNumber(Long.parseLong(current.toString()));
                        long blockNum = blockInfo.getBlockNumber();
                        for(EnvelopeInfo envelopeInfo : blockInfo.getEnvelopeInfos()) {
                            String transactionId = envelopeInfo.getTransactionID();
                            TransactionInfo transactionInfo = channel.queryTransactionByID(transactionId);
                            TransactionObject transactionObject = new TransactionObject(blockNum, transactionInfo);
                            List<TransactionAction> transactionActions = transactionObject.getDeserializer().getTransactionActions();
                            for(int i = 0 ; i < transactionActions.size() ; i++) {
                                TxReadWriteSet readWriteSet = new TransactionActionDeserializer(transactionActions.get(i)).getTxReadWriteSet();
                                for(int j = 0 ; j < readWriteSet.getNsRwsetCount() ; j++) {
                                    NsReadWriteSet ns = readWriteSet.getNsRwset(j);
                                    String namespace = ns.getNamespace();
                                    KVRWSet set = KVRWSet.parseFrom(ns.getRwset());
                                    for(int k = 0 ; k < set.getReadsCount() ; k++) {
                                        KVRead read = set.getReads(k);
                                        String key = Integer.toString(new Tuple2<>(new Tuple2<>(transactionId, i), new Tuple3<>(j, "read", k)).hashCode());
                                        if(dataMap.get(key) != null) {
                                            keys.add(key);
                                        } else {
                                            dataMap.put(key, new ReadWriteSetObject(blockNum, transactionId, namespace, read));
                                            keys.add(key);
                                        }
                                    }
                                    for(int k = 0 ; k < set.getRangeQueriesInfoCount() ; k++) {
                                        RangeQueryInfo rangeQueryInfo = set.getRangeQueriesInfo(k);
                                        String key = Integer.toString(new Tuple2<>(new Tuple2<>(transactionId, i), new Tuple3<>(j, "rangeQueryInfo", k)).hashCode());
                                        if(dataMap.get(key) != null) {
                                            keys.add(key);
                                        } else {
                                            dataMap.put(key, new ReadWriteSetObject(blockNum, transactionId, namespace, rangeQueryInfo));
                                            keys.add(key);
                                        }
                                    }
                                    for(int k = 0 ; k < set.getWritesCount() ; k++) {
                                        KVWrite write = set.getWrites(k);
                                        String key = Integer.toString(new Tuple2<>(new Tuple2<>(transactionId, i), new Tuple3<>(j, "write", k)).hashCode());
                                        if(dataMap.get(key) != null) {
                                            keys.add(key);
                                        } else {
                                            dataMap.put(key, new ReadWriteSetObject(blockNum, transactionId, namespace, write));
                                            keys.add(key);
                                        }
                                    }
                                }
                            }
                        }
                    } catch (Exception e) {
                        throw new BlkchnException("Error query block by number " + current, e);
                    }
                }
                current = rangeOps.add(current, 1);
            } while (max.compareTo(current) >= 0);
            return new DataNode<>(rangeTable, keys);
        }).collect(Collectors.toList());
        DataNode<String> finalDataNode = dataNodes.get(0);
        if (dataNodes.size() > 1) {
            for (int i = 1; i < dataNodes.size(); i++) {
                finalDataNode = mergeDataNodes(finalDataNode, dataNodes.get(i), Operator.OR);
            }
        }
        return finalDataNode;
    }

    @SuppressWarnings("unchecked")
    protected <T extends Number & Comparable<T>> TreeNode combineRangeAndDataNodes(RangeNode<T> rangeNode,
            DataNode<?> dataNode, LogicalOperation oper) {
        String tableName = dataNode.getTable();
        List<String> keys = dataNode.getKeys().stream().map(x -> x.toString()).collect(Collectors.toList());
        String rangeCol = rangeNode.getColumn();
        RangeOperations<T> rangeOps = (RangeOperations<T>) physicalPlan.getRangeOperations(tableName, rangeCol);
        if (FabricTables.BLOCK.equals(tableName)) {
            if (FabricColumns.BLOCK_NO.equals(rangeCol)) {
                List<RangeNode<T>> dataRanges = keys.stream().map(key -> {
                    BlockInfo blockInfo = (BlockInfo) dataMap.get(key);
                    if (auxillaryDataMap.containsKey(FabricColumns.BLOCK_NO)) {
                        auxillaryDataMap.get(FabricColumns.BLOCK_NO).put(key, blockInfo);
                    } else {
                        auxillaryDataMap.put(FabricColumns.BLOCK_NO, new HashMap<>());
                        auxillaryDataMap.get(FabricColumns.BLOCK_NO).put(key, blockInfo);
                    }
                    T blockNo = (T) new Long(blockInfo.getBlockNumber());
                    RangeNode<T> node = new RangeNode<>(rangeNode.getTable(), rangeCol);
                    node.getRangeList().addRange(new Range<T>(blockNo, blockNo));
                    return node;
                }).collect(Collectors.toList());
                if(dataRanges.isEmpty()) {
                    return rangeNode;
                }
                RangeNode<T> dataRangeNodes = dataRanges.get(0);
                if (dataRanges.size() > 1) {
                    for (int i = 1; i < dataRanges.size(); i++) {
                        dataRangeNodes = rangeOps.rangeNodeOr(dataRangeNodes, dataRanges.get(i));
                    }
                }
                if (oper.isAnd()) {
                    return rangeOps.rangeNodeAnd(dataRangeNodes, rangeNode);
                } else {
                    return rangeOps.rangeNodeOr(dataRangeNodes, rangeNode);
                }
            }
        } else if(FabricColumns.BLOCK_NO.equals(rangeCol)) {
            if(oper.isOr()) {
                LogicalOperation newOper = new LogicalOperation(Operator.OR);
                newOper.addChildNode(dataNode);
                newOper.addChildNode(rangeNode);
                return newOper;
            } else {
                return filterRangeNodeWithValue(rangeNode, dataNode);
            }
        }
        RangeNode<T> emptyRangeNode = new RangeNode<>(rangeNode.getTable(), rangeNode.getColumn());
        emptyRangeNode.getRangeList().addRange(new Range<T>(rangeOps.getMinValue(), rangeOps.getMinValue()));
        return emptyRangeNode;
    }

    protected boolean filterField(String fieldName, Object obj, String value, Comparator comparator) {
        boolean retValue = false;
        if (obj instanceof BlockInfo) {
            retValue = filterFieldBlock(fieldName, obj, value, comparator);
        } else if(obj instanceof TransactionObject) {
            retValue = filterFieldTransaction(fieldName, obj, value, comparator);
        } else if(obj instanceof TransactionActionObject) {
            retValue = filterFieldTransactionAction(fieldName, obj, value, comparator);
        } else if(obj instanceof ReadWriteSetObject) {
            retValue = filterFieldReadWriteSet(fieldName, obj, value, comparator);
        }
        return retValue;
    }

    protected <T> DataNode<T> filterRangeNodeWithValue(RangeNode<?> rangeNode, DataNode<T> dataNode) {
        List<T> filteredKeys = dataNode.getKeys().stream().filter(key -> {
            if (FabricTables.BLOCK.equals(dataNode.getTable()) && FabricColumns.BLOCK_NO.equals(rangeNode.getColumn())) {
                boolean include = false;
                Long longKey = (Long) key;
                for (Range<?> range : rangeNode.getRangeList().getRanges()) {
                    if (((Long) range.getMin()) <= longKey && ((Long) range.getMax()) >= longKey) {
                        include = true;
                        break;
                    }
                }
                return include;
            } else if(FabricTables.TRANSACTION.equals(dataNode.getTable()) && FabricColumns.BLOCK_NO.equals(rangeNode.getColumn())) {
                boolean include = false;
                TransactionObject transaction = (TransactionObject) dataMap.get(key);
                Long blockNo = transaction.getBlockNo();
                for (Range<?> range : rangeNode.getRangeList().getRanges()) {
                    if (((Long) range.getMin()) <= blockNo && ((Long) range.getMax()) >= blockNo) {
                        include = true;
                        break;
                    }
                }
                return include;
            } else if(FabricTables.TRANSACTION_ACTION.equals(dataNode.getTable()) && FabricColumns.BLOCK_NO.equals(rangeNode.getColumn())) {
                boolean include = false;
                TransactionActionObject transactionAction = (TransactionActionObject) dataMap.get(key);
                Long blockNo = transactionAction.getBlockNo();
                for (Range<?> range : rangeNode.getRangeList().getRanges()) {
                    if (((Long) range.getMin()) <= blockNo && ((Long) range.getMax()) >= blockNo) {
                        include = true;
                        break;
                    }
                }
                return include;
            } else if(FabricTables.READ_WRITE_SET.equals(dataNode.getTable()) && FabricColumns.BLOCK_NO.equals(rangeNode.getColumn())) {
                boolean include = false;
                ReadWriteSetObject readWriteSet = (ReadWriteSetObject) dataMap.get(key);
                Long blockNo = readWriteSet.getBlockNo();
                for (Range<?> range : rangeNode.getRangeList().getRanges()) {
                    if (((Long) range.getMin()) <= blockNo && ((Long) range.getMax()) >= blockNo) {
                        include = true;
                        break;
                    }
                }
                return include;
            }
            return false;
        }).collect(Collectors.toList());
        return new DataNode<>(dataNode.getTable(), filteredKeys);
    }

    protected DataFrame createDataFrame(DataNode<?> dataNode) {
        if(dataNode.getKeys().isEmpty()) {
            return new DataFrame(new ArrayList<>(), new ArrayList<>(), physicalPlan.getColumnAliasMapping());
        }
        if (dataMap.get(dataNode.getKeys().get(0).toString()) instanceof BlockInfo) {
            String[] columns = FabricPhysicalPlan.getFabricTableColumnMap().get(FabricTables.BLOCK).toArray(new String[]{});
            List<List<Object>> data = new ArrayList<>();
            for (Object key : dataNode.getKeys()) {
                BlockInfo blockInfo = (BlockInfo) dataMap.get(key.toString());
                String previousHash = Hex.encodeHexString(blockInfo.getPreviousHash());
                String dataHash = Hex.encodeHexString(blockInfo.getDataHash());
                String transActionsMetaData = Hex.encodeHexString(blockInfo.getTransActionsMetaData());
                int transactionCount = blockInfo.getEnvelopeCount();
                long blockNum = blockInfo.getBlockNumber();
                String channelId;
                try {
                    channelId = blockInfo.getChannelId();
                } catch (InvalidProtocolBufferException e) {
                    throw new BlkchnException("Unable to get channel id from block info", e);
                }
                data.add(Arrays.asList(previousHash, dataHash, transActionsMetaData, transactionCount, blockNum, channelId));
            }
            DataFrame df = new DataFrame(data, columns, physicalPlan.getColumnAliasMapping());
            df.setRawData(dataMap.values());
            return df;
        } else if(dataMap.get(dataNode.getKeys().get(0).toString()) instanceof TransactionObject) {
            String[] columns = FabricPhysicalPlan.getFabricTableColumnMap().get(FabricTables.TRANSACTION).toArray(new String[]{});
            List<List<Object>> data = new ArrayList<>();
            for(Object key : dataNode.getKeys()) {
                TransactionObject transactionObject = (TransactionObject) dataMap.get(key.toString());
                TransactionDeserializer transactionDeserializer = transactionObject.getDeserializer();
                data.add(Arrays.asList(transactionObject.getBlockNo(), transactionDeserializer.getTransactionId(), transactionDeserializer.getHeaderType(), 
                        transactionDeserializer.getMessageProtocolVersion(), transactionDeserializer.getTimestamp(), transactionDeserializer.getEpoch(), 
                        transactionDeserializer.getChannelId(), transactionDeserializer.getCreatorMSP(), transactionDeserializer.getCreatorSignature(), 
                        transactionDeserializer.getNonce()));
            }
            DataFrame df = new DataFrame(data, columns, physicalPlan.getColumnAliasMapping());
            df.setRawData(dataMap.values());
            return df;
        } else if(dataMap.get(dataNode.getKeys().get(0).toString()) instanceof TransactionActionObject) {
            String[] columns = FabricPhysicalPlan.getFabricTableColumnMap().get(FabricTables.TRANSACTION_ACTION).toArray(new String[]{});
            List<List<Object>> data = new ArrayList<>();
            for(Object key : dataNode.getKeys()) {
                TransactionActionObject transactionActionObject = (TransactionActionObject) dataMap.get(key.toString());
                TransactionActionDeserializer actionDeserializer = transactionActionObject.getDeserializer();
                data.add(Arrays.asList(transactionActionObject.getBlockNo(), transactionActionObject.getTransactionId(), actionDeserializer.getIdGenerationAlg(),
                    actionDeserializer.getChaincodeType(), actionDeserializer.getChaincodeName(), actionDeserializer.getChaincodeVersion(),
                    actionDeserializer.getChaincodePath(), actionDeserializer.getChaincodeArgs(), actionDeserializer.getTimeOut(),
                    actionDeserializer.getRWDataModel(), actionDeserializer.getResponseMessage(), actionDeserializer.getResponseStatus(),
                    actionDeserializer.getResponsePayload(), actionDeserializer.getEndorsements()));
            }
            DataFrame df = new DataFrame(data, columns, physicalPlan.getColumnAliasMapping());
            df.setRawData(dataMap.values());
            return df;
        } else if(dataMap.get(dataNode.getKeys().get(0).toString()) instanceof ReadWriteSetObject) {
            String[] columns = FabricPhysicalPlan.getFabricTableColumnMap().get(FabricTables.READ_WRITE_SET).toArray(new String[]{});
            List<List<Object>> data = new ArrayList<>();
            for(Object key : dataNode.getKeys()) {
                ReadWriteSetObject readWriteSetObject = (ReadWriteSetObject) dataMap.get(key.toString());
                ReadWriteSetDeserializer deserializer = readWriteSetObject.getDeserializer();
                data.add(Arrays.asList(readWriteSetObject.getBlockNo(), readWriteSetObject.getTransactionId(), readWriteSetObject.getNamespace(), deserializer.getReadKey(),
                        deserializer.getReadBlockNo(), deserializer.getReadTxNum(), deserializer.getRangeQueryStartKey(), deserializer.getRangeQueryEndKey(),
                        deserializer.getRangeQueryItrExausted(), deserializer.getRangeQueryReadsInfo(), deserializer.getWriteKey(), deserializer.getIsDelete(),
                        deserializer.getWriteValue()));
            }
            DataFrame df = new DataFrame(data, columns, physicalPlan.getColumnAliasMapping());
            df.setRawData(dataMap.values());
            return df;
        } else {
            throw new BlkchnException("Cannot create dataframe from unknown object type");
        }
    }
    
    private boolean filterFieldBlock(String fieldName, Object obj, String value, Comparator comparator) {
        BlockInfo blockInfo = (BlockInfo) obj;
        boolean retValue;
        switch (fieldName) {
            case FabricColumns.BLOCK_DATA_HASH:
                if (!comparator.isEQ() && !comparator.isNEQ()) {
                    throw new BlkchnException(String.format(
                            "String values in %s field can only be compared for equivalence and non-equivalence", fieldName));
                }
                if(comparator.isEQ()) {
                    retValue = Hex.encodeHexString(blockInfo.getDataHash()).equals(value.replaceAll("'", ""));
                } else {
                    retValue = !Hex.encodeHexString(blockInfo.getDataHash()).equals(value.replaceAll("'", ""));
                }
                break;

            case FabricColumns.TRANS_ACTIONS_META_DATA:
                if (!comparator.isEQ() && !comparator.isNEQ()) {
                    throw new BlkchnException(String.format(
                            "String values in %s field can only be compared for equivalence and non-equivalence", fieldName));
                }
                if(comparator.isEQ()) {
                    retValue = Hex.encodeHexString(blockInfo.getTransActionsMetaData()).equals(value.replaceAll("'", ""));
                } else {
                    retValue = !Hex.encodeHexString(blockInfo.getTransActionsMetaData()).equals(value.replaceAll("'", ""));
                }
                break;

            case FabricColumns.TRANSACTION_COUNT:
                Number transactionCount = blockInfo.getEnvelopeCount();
                retValue = compareNumbers(transactionCount, Integer.parseInt(value), comparator);
                break;

            case FabricColumns.CHANNEL_ID:
                try {
                    if (!comparator.isEQ() && !comparator.isNEQ()) {
                        throw new BlkchnException(String.format(
                                "String values in %s field can only be compared for equivalence and non-equivalence", fieldName));
                    }
                    if(comparator.isEQ()) {
                        retValue = blockInfo.getChannelId().equals(value.replaceAll("'", ""));
                    } else {
                        retValue = !blockInfo.getChannelId().equals(value.replaceAll("'", ""));
                    }
                } catch (InvalidProtocolBufferException e) {
                    throw new BlkchnException("Error fetching channel id", e);
                }
                break;
                
            case FabricColumns.PREVIOUS_HASH:
                if (!comparator.isEQ() && !comparator.isNEQ()) {
                    throw new BlkchnException(String.format(
                            "String values in %s field can only be compared for equivalence and non-equivalence", fieldName));
                }
                if(comparator.isEQ()) {
                    retValue = Hex.encodeHexString(blockInfo.getPreviousHash()).equals(value.replaceAll("'", ""));
                } else {
                    retValue = !Hex.encodeHexString(blockInfo.getPreviousHash()).equals(value.replaceAll("'", ""));
                }
                break;

            default:
                retValue = false;
        }
        return retValue;
    }
    
    private boolean filterFieldTransaction(String fieldName, Object obj, String value, Comparator comparator) {
        boolean retValue;
        TransactionObject transaction = (TransactionObject) obj;
        switch(fieldName) {
            case FabricColumns.TRANSACTION_ID: 
                if (!comparator.isEQ() && !comparator.isNEQ()) {
                    throw new BlkchnException(String.format(
                            "String values in %s field can only be compared for equivalence and non-equivalence", fieldName));
                }
                if(comparator.isEQ()) {
                    retValue = transaction.getDeserializer().getTransactionId().equals(value.replaceAll("'", ""));
                } else {
                    retValue = !transaction.getDeserializer().getTransactionId().equals(value.replaceAll("'", ""));
                }
                break;
                
            case FabricColumns.HEADER_TYPE:
                retValue = compareNumbers(transaction.getDeserializer().getHeaderType(), Integer.parseInt(value), comparator);
                break;
                
            case FabricColumns.MESSAGE_PROTOCOL_VERSION:
                retValue = compareNumbers(transaction.getDeserializer().getMessageProtocolVersion(), Integer.parseInt(value), comparator);
                break;
                
            case FabricColumns.TIMESTAMP:
                DateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                df.setLenient(false);
                long seconds;
                try {
                    seconds = df.parse(value.replaceAll("'", "")).getTime() / 1000;
                } catch (ParseException e) {
                    throw new BlkchnException(e);
                }
                retValue = compareNumbers(transaction.getDeserializer().getTimestamp().getSeconds(), seconds, comparator);
                break;
                
            case FabricColumns.CHANNEL_ID:
                if (!comparator.isEQ() && !comparator.isNEQ()) {
                    throw new BlkchnException(String.format(
                            "String values in %s field can only be compared for equivalence and non-equivalence", fieldName));
                }
                if(comparator.isEQ()) {
                    retValue = transaction.getDeserializer().getChannelId().equals(value.replaceAll("'", ""));
                } else {
                    retValue = !transaction.getDeserializer().getChannelId().equals(value.replaceAll("'", ""));
                }
                break;
                
            case FabricColumns.EPOCH:
                retValue = compareNumbers(transaction.getDeserializer().getEpoch(), Long.parseLong(value), comparator);
                break;
                
            case FabricColumns.CREATOR_MSP:
                if (!comparator.isEQ() && !comparator.isNEQ()) {
                    throw new BlkchnException(String.format(
                            "String values in %s field can only be compared for equivalence and non-equivalence", fieldName));
                }
                if(comparator.isEQ()) {
                    retValue = transaction.getDeserializer().getCreatorMSP().equals(value.replaceAll("'", ""));
                } else {
                    retValue = !transaction.getDeserializer().getCreatorMSP().equals(value.replaceAll("'", ""));
                }
                break;
                
            case FabricColumns.CREATOR_SIGNATURE:
                if (!comparator.isEQ() && !comparator.isNEQ()) {
                    throw new BlkchnException(String.format(
                            "String values in %s field can only be compared for equivalence and non-equivalence", fieldName));
                }
                if(comparator.isEQ()) {
                    retValue = transaction.getDeserializer().getCreatorSignature().equals(value.replaceAll("'", ""));
                } else {
                    retValue = !transaction.getDeserializer().getCreatorSignature().equals(value.replaceAll("'", ""));
                }
                break;
                
            case FabricColumns.NONCE:
                if (!comparator.isEQ() && !comparator.isNEQ()) {
                    throw new BlkchnException(String.format(
                            "String values in %s field can only be compared for equivalence and non-equivalence", fieldName));
                }
                if(comparator.isEQ()) {
                    retValue = transaction.getDeserializer().getNonce().equals(value.replaceAll("'", ""));
                } else {
                    retValue = !transaction.getDeserializer().getNonce().equals(value.replaceAll("'", ""));
                }
                break;
                
            default:
                retValue = false;
        }
        return retValue;
    }
    
    private boolean filterFieldTransactionAction(String fieldName, Object obj, String value, Comparator comparator) {
        boolean retValue;
        TransactionActionObject transactionAction = (TransactionActionObject) obj;
        switch(fieldName) {
            case FabricColumns.TRANSACTION_ID:
                if (!comparator.isEQ() && !comparator.isNEQ()) {
                    throw new BlkchnException(String.format(
                            "String values in %s field can only be compared for equivalence and non-equivalence", fieldName));
                }
                if(comparator.isEQ()) {
                    retValue = transactionAction.getTransactionId().equals(value.replaceAll("'", ""));
                } else {
                    retValue = !transactionAction.getTransactionId().equals(value.replaceAll("'", ""));
                }
                break;
                
            case FabricColumns.ID_GENERATION_ALG:
                if (!comparator.isEQ() && !comparator.isNEQ()) {
                    throw new BlkchnException(String.format(
                            "String values in %s field can only be compared for equivalence and non-equivalence", fieldName));
                }
                if(comparator.isEQ()) {
                    retValue = value.replaceAll("'", "").equals(transactionAction.getDeserializer().getIdGenerationAlg());
                } else {
                    retValue = !value.replaceAll("'", "").equals(transactionAction.getDeserializer().getIdGenerationAlg());
                }
                break;
                
            case FabricColumns.CHAINCODE_TYPE:
                if (!comparator.isEQ() && !comparator.isNEQ()) {
                    throw new BlkchnException(String.format(
                            "String values in %s field can only be compared for equivalence and non-equivalence", fieldName));
                }
                if(comparator.isEQ()) {
                    retValue = value.replaceAll("'", "").equals(transactionAction.getDeserializer().getChaincodeType());
                } else {
                    retValue = !value.replaceAll("'", "").equals(transactionAction.getDeserializer().getChaincodeType());
                }
                break;
                
            case FabricColumns.CHAINCODE_NAME:
                if (!comparator.isEQ() && !comparator.isNEQ()) {
                    throw new BlkchnException(String.format(
                            "String values in %s field can only be compared for equivalence and non-equivalence", fieldName));
                }
                if(comparator.isEQ()) {
                    retValue = value.replaceAll("'", "").equals(transactionAction.getDeserializer().getChaincodeName());
                } else {
                    retValue = !value.replaceAll("'", "").equals(transactionAction.getDeserializer().getChaincodeName());
                }
                break;
                
            case FabricColumns.CHAINCODE_VERSION:
                if (!comparator.isEQ() && !comparator.isNEQ()) {
                    throw new BlkchnException(String.format(
                            "String values in %s field can only be compared for equivalence and non-equivalence", fieldName));
                }
                if(comparator.isEQ()) {
                    retValue = value.replaceAll("'", "").equals(transactionAction.getDeserializer().getChaincodeVersion());
                } else {
                    retValue = !value.replaceAll("'", "").equals(transactionAction.getDeserializer().getChaincodeVersion());
                }
                break;
                
            case FabricColumns.CHAINCODE_PATH:
                if (!comparator.isEQ() && !comparator.isNEQ()) {
                    throw new BlkchnException(String.format(
                            "String values in %s field can only be compared for equivalence and non-equivalence", fieldName));
                }
                if(comparator.isEQ()) {
                    retValue = value.replaceAll("'", "").equals(transactionAction.getDeserializer().getChaincodePath());
                } else {
                    retValue = !value.replaceAll("'", "").equals(transactionAction.getDeserializer().getChaincodePath());
                }
                break;
                
            case FabricColumns.TIME_OUT:
                retValue = compareNumbers(transactionAction.getDeserializer().getTimeOut(), Integer.parseInt(value), comparator);
                break;
                
            case FabricColumns.RW_DATAMODEL:
                if (!comparator.isEQ() && !comparator.isNEQ()) {
                    throw new BlkchnException(String.format(
                            "String values in %s field can only be compared for equivalence and non-equivalence", fieldName));
                }
                if(comparator.isEQ()) {
                    retValue = value.replaceAll("'", "").equals(transactionAction.getDeserializer().getRWDataModel());
                } else {
                    retValue = !value.replaceAll("'", "").equals(transactionAction.getDeserializer().getRWDataModel());
                }
                break;
                
            case FabricColumns.RESPONSE_MESSAGE:
                if (!comparator.isEQ() && !comparator.isNEQ()) {
                    throw new BlkchnException(String.format(
                            "String values in %s field can only be compared for equivalence and non-equivalence", fieldName));
                }
                if(comparator.isEQ()) {
                    retValue = value.replaceAll("'", "").equals(transactionAction.getDeserializer().getResponseMessage());
                } else {
                    retValue = !value.replaceAll("'", "").equals(transactionAction.getDeserializer().getResponseMessage());
                }
                break;
                
            case FabricColumns.RESPONSE_STATUS:
                retValue = compareNumbers(transactionAction.getDeserializer().getResponseStatus(), Integer.parseInt(value), comparator);
                break;
                
            case FabricColumns.RESPONSE_PAYLOAD:
                if (!comparator.isEQ() && !comparator.isNEQ()) {
                    throw new BlkchnException(String.format(
                            "String values in %s field can only be compared for equivalence and non-equivalence", fieldName));
                }
                if(comparator.isEQ()) {
                    retValue = value.replaceAll("'", "").equals(transactionAction.getDeserializer().getResponsePayload());
                } else {
                    retValue = !value.replaceAll("'", "").equals(transactionAction.getDeserializer().getResponsePayload());
                }
                break;
                
            default:
                retValue = false;
        }
        return retValue;
    }
    
    private boolean filterFieldReadWriteSet(String fieldName, Object obj, String value, Comparator comparator) {
        boolean retValue;
        ReadWriteSetObject readWriteSet = (ReadWriteSetObject) obj;
        switch(fieldName) {
            case FabricColumns.TRANSACTION_ID:
                if (!comparator.isEQ() && !comparator.isNEQ()) {
                    throw new BlkchnException(String.format(
                            "String values in %s field can only be compared for equivalence and non-equivalence", fieldName));
                }
                if(comparator.isEQ()) {
                    retValue = value.replaceAll("'", "").equals(readWriteSet.getTransactionId());
                } else {
                    retValue = !value.replaceAll("'", "").equals(readWriteSet.getTransactionId());
                }
                break;
                
            case FabricColumns.NAMESPACE:
                if (!comparator.isEQ() && !comparator.isNEQ()) {
                    throw new BlkchnException(String.format(
                            "String values in %s field can only be compared for equivalence and non-equivalence", fieldName));
                }
                if(comparator.isEQ()) {
                    retValue = value.replaceAll("'", "").equals(readWriteSet.getNamespace());
                } else {
                    retValue = !value.replaceAll("'", "").equals(readWriteSet.getNamespace());
                }
                break;
                
            case FabricColumns.READ_KEY:
                if (!comparator.isEQ() && !comparator.isNEQ()) {
                    throw new BlkchnException(String.format(
                            "String values in %s field can only be compared for equivalence and non-equivalence", fieldName));
                }
                if(comparator.isEQ()) {
                    retValue = value.replaceAll("'", "").equals(readWriteSet.getDeserializer().getReadKey());
                } else {
                    retValue = !value.replaceAll("'", "").equals(readWriteSet.getDeserializer().getReadKey());
                }
                break;
                
            case FabricColumns.READ_BLOCK_NO:
                retValue = compareNumbers(readWriteSet.getDeserializer().getReadBlockNo(), Long.parseLong(value), comparator);
                break;
                
            case FabricColumns.READ_TX_NUM:
                retValue = compareNumbers(readWriteSet.getDeserializer().getReadTxNum(), Long.parseLong(value), comparator);
                break;
                
            case FabricColumns.RANGE_QUERY_START_KEY:
                if (!comparator.isEQ() && !comparator.isNEQ()) {
                    throw new BlkchnException(String.format(
                            "String values in %s field can only be compared for equivalence and non-equivalence", fieldName));
                }
                if(comparator.isEQ()) {
                    retValue = value.replaceAll("'", "").equals(readWriteSet.getDeserializer().getRangeQueryStartKey());
                } else {
                    retValue = !value.replaceAll("'", "").equals(readWriteSet.getDeserializer().getRangeQueryStartKey());
                }
                break;
                
            case FabricColumns.RANGE_QUERY_END_KEY:
                if (!comparator.isEQ() && !comparator.isNEQ()) {
                    throw new BlkchnException(String.format(
                            "String values in %s field can only be compared for equivalence and non-equivalence", fieldName));
                }
                if(comparator.isEQ()) {
                    retValue = value.replaceAll("'", "").equals(readWriteSet.getDeserializer().getRangeQueryEndKey());
                } else {
                    retValue = !value.replaceAll("'", "").equals(readWriteSet.getDeserializer().getRangeQueryEndKey());
                }
                break;
                
            case FabricColumns.RANGE_QUERY_ITR_EXAUSTED:
                if (!comparator.isEQ() && !comparator.isNEQ()) {
                    throw new BlkchnException(String.format(
                            "boolean values in %s field can only be compared for equivalence and non-equivalence", fieldName));
                }
                if(comparator.isEQ()) {
                    retValue = Boolean.parseBoolean(value) == readWriteSet.getDeserializer().getRangeQueryItrExausted();
                } else {
                    retValue = Boolean.parseBoolean(value) != readWriteSet.getDeserializer().getRangeQueryItrExausted();
                }
                break;
                
            case FabricColumns.RANGE_QUERY_READS_INFO:
                if (!comparator.isEQ() && !comparator.isNEQ()) {
                    throw new BlkchnException(String.format(
                            "String values in %s field can only be compared for equivalence and non-equivalence", fieldName));
                }
                if(comparator.isEQ()) {
                    retValue = value.replaceAll("'", "").equals(readWriteSet.getDeserializer().getRangeQueryReadsInfo());
                } else {
                    retValue = !value.replaceAll("'", "").equals(readWriteSet.getDeserializer().getRangeQueryReadsInfo());
                }
                break;
                
            case FabricColumns.WRITE_KEY:
                if (!comparator.isEQ() && !comparator.isNEQ()) {
                    throw new BlkchnException(String.format(
                            "String values in %s field can only be compared for equivalence and non-equivalence", fieldName));
                }
                if(comparator.isEQ()) {
                    retValue = value.replaceAll("'", "").equals(readWriteSet.getDeserializer().getWriteKey());
                } else {
                    retValue = !value.replaceAll("'", "").equals(readWriteSet.getDeserializer().getWriteKey());
                }
                break;
                
            case FabricColumns.IS_DELETE:
                if (!comparator.isEQ() && !comparator.isNEQ()) {
                    throw new BlkchnException(String.format(
                            "boolean values in %s field can only be compared for equivalence and non-equivalence", fieldName));
                }
                if(comparator.isEQ()) {
                    retValue = Boolean.parseBoolean(value) == readWriteSet.getDeserializer().getIsDelete();
                } else {
                    retValue = Boolean.parseBoolean(value) != readWriteSet.getDeserializer().getIsDelete();
                }
                break;
                
            case FabricColumns.WRITE_VALUE:
                if (!comparator.isEQ() && !comparator.isNEQ()) {
                    throw new BlkchnException(String.format(
                            "String values in %s field can only be compared for equivalence and non-equivalence", fieldName));
                }
                if(comparator.isEQ()) {
                    retValue = value.replaceAll("'", "").equals(readWriteSet.getDeserializer().getWriteValue());
                } else {
                    retValue = !value.replaceAll("'", "").equals(readWriteSet.getDeserializer().getWriteValue());
                }
                break;
                
            default: 
                retValue = false;
        }
        return retValue;
    }

}
