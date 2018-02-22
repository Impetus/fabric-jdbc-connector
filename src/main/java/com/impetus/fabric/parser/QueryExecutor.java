package com.impetus.fabric.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.hyperledger.fabric.sdk.BlockInfo;
import org.hyperledger.fabric.sdk.Channel;
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
            throw new BlkchnException("This query can't be executed");
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
                System.out.println("-- Executing Range Node");
                finalData = executeRangeNode(rangeNode);
                System.out.println("DataNode: ");
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
        Channel channel = queryBlock.reconstructChannel();
        if (table.equals("block") && column.equals("blockNo")) {
            try {
                BlockInfo blockInfo = channel.queryBlockByNumber(Long.parseLong(value));
                dataMap.put(Long.toString(blockInfo.getBlockNumber()), blockInfo);
                return new DataNode<>(table, Arrays.asList(Long.toString(blockInfo.getBlockNumber())));
            } catch (NumberFormatException | InvalidArgumentException | ProposalException e) {
                throw new BlkchnException("Error querying block by block number " + value, e);
            }
        } else if (table.equals("block") && column.equals("previousHash")) {
            try {
                BlockInfo blockInfo = channel.queryBlockByHash(Hex.decodeHex(value.replace("'", "").toCharArray()));
                dataMap.put(Long.toString(blockInfo.getBlockNumber()), blockInfo);
                return new DataNode<>(table, Arrays.asList(Long.toString(blockInfo.getBlockNumber())));
            } catch (InvalidArgumentException | ProposalException | DecoderException e) {
                throw new BlkchnException("Error querying block by hash " + value.replace("'", ""), e);
            }
        } else if (table.equals("block") && column.equals("transactionId")) {
            try {
                BlockInfo blockInfo = channel.queryBlockByTransactionID(value.replace("'", ""));
                dataMap.put(Long.toString(blockInfo.getBlockNumber()), blockInfo);
                return new DataNode<>(table, Arrays.asList(Long.toString(blockInfo.getBlockNumber())));
            } catch (InvalidArgumentException | ProposalException e) {
                throw new BlkchnException("Error querying block by transaction id " + value.replace("'", ""), e);
            }
        } else {
            throw new BlkchnException(String.format("There is no direct API for table %s and column %s combination",
                    table, column));
        }
    }

    @SuppressWarnings("unchecked")
    protected <T extends Number & Comparable<T>> DataNode<T> executeRangeNode(RangeNode<T> rangeNode) {
        if(rangeNode.getRangeList().getRanges().isEmpty()) {
            return new DataNode<>(rangeNode.getTable(), new ArrayList<>());
        }
        RangeOperations<T> rangeOps = (RangeOperations<T>) physicalPlan.getRangeOperations(rangeNode.getTable(),
                rangeNode.getColumn());
        String rangeCol = rangeNode.getColumn();
        String rangeTable = rangeNode.getTable();
        Channel channel = queryBlock.reconstructChannel();
        Long height;
        try {
            height = channel.queryBlockchainInfo().getHeight();
        } catch (ProposalException | InvalidArgumentException e) {
            throw new BlkchnException("Error getting height of ledger", e);
        }
        List<DataNode<T>> dataNodes = rangeNode.getRangeList().getRanges().stream().map(range -> {
            List<T> keys = new ArrayList<>();
            T current = range.getMin().equals(rangeOps.getMinValue()) ? (T) new Long(0l) : range.getMin();
            T max = range.getMax().equals(rangeOps.getMaxValue()) ? (T) (new Long(height -1)) : range.getMax();
            do {
                if ("block".equals(rangeTable) && "blockNo".equals(rangeCol)) {
                    try {
                        if (dataMap.get(current.toString()) != null) {
                            keys.add(current);
                        } else {
                            BlockInfo blockInfo = channel.queryBlockByNumber(Long.parseLong(current.toString()));
                            dataMap.put(Long.toString(blockInfo.getBlockNumber()), blockInfo);
                            keys.add(current);
                        }
                    } catch (Exception e) {
                        throw new BlkchnException("Error query block by number " + current, e);
                    }
                }
                current = rangeOps.add(current, 1);
            } while (max.compareTo(current) >= 0);
            return new DataNode<>(rangeTable, keys);
        }).collect(Collectors.toList());
        DataNode<T> finalDataNode = dataNodes.get(0);
        if (dataNodes.size() > 1) {
            for (int i = 1; i < dataNodes.size(); i++) {
                finalDataNode = mergeDataNodes(finalDataNode, dataNodes.get(i), Operator.OR);
            }
        }
        return finalDataNode;
    }

    @SuppressWarnings("unchecked")
    protected <T extends Number & Comparable<T>> RangeNode<T> combineRangeAndDataNodes(RangeNode<T> rangeNode,
            DataNode<?> dataNode, LogicalOperation oper) {
        String tableName = dataNode.getTable();
        List<String> keys = dataNode.getKeys().stream().map(x -> x.toString()).collect(Collectors.toList());
        String rangeCol = rangeNode.getColumn();
        RangeOperations<T> rangeOps = (RangeOperations<T>) physicalPlan.getRangeOperations(tableName, rangeCol);
        if ("block".equals(tableName)) {
            if ("blockNo".equals(rangeCol)) {
                List<RangeNode<T>> dataRanges = keys.stream().map(key -> {
                    BlockInfo blockInfo = (BlockInfo) dataMap.get(key);
                    if (auxillaryDataMap.containsKey("blockNo")) {
                        auxillaryDataMap.get("blockNo").put(key, blockInfo);
                    } else {
                        auxillaryDataMap.put("blockNo", new HashMap<>());
                        auxillaryDataMap.get("blockNo").put(key, blockInfo);
                    }
                    T blockNo = (T) new Long(blockInfo.getBlockNumber());
                    RangeNode<T> node = new RangeNode<>(rangeNode.getTable(), rangeCol);
                    node.getRangeList().addRange(new Range<T>(blockNo, blockNo));
                    return node;
                }).collect(Collectors.toList());
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
        }
        RangeNode<T> emptyRangeNode = new RangeNode<>(rangeNode.getTable(), rangeNode.getColumn());
        emptyRangeNode.getRangeList().addRange(new Range<T>(rangeOps.getMinValue(), rangeOps.getMinValue()));
        return emptyRangeNode;
    }

    protected boolean filterField(String fieldName, Object obj, String value, Comparator comparator) {
        boolean retValue = false;
        if (obj instanceof BlockInfo) {
            BlockInfo blockInfo = (BlockInfo) obj;
            switch (fieldName) {
                case "blockDataHash":
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

                case "transActionsMetaData":
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

                case "transactionCount":
                    Number transactionCount = blockInfo.getEnvelopeCount();
                    retValue = compareNumbers(transactionCount, Integer.parseInt(value), comparator);
                    break;

                case "channelId":
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

                default:
                    retValue = false;
            }
        }
        return retValue;
    }

    protected <T> DataNode<T> filterRangeNodeWithValue(RangeNode<?> rangeNode, DataNode<T> dataNode) {
        List<T> filteredKeys = dataNode.getKeys().stream().filter(key -> {
            if ("block".equals(dataNode.getTable()) && "blockNo".equals(rangeNode.getColumn())) {
                boolean include = false;
                Long longKey = (Long) key;
                for (Range<?> range : rangeNode.getRangeList().getRanges()) {
                    if (((Long) range.getMin()) <= longKey && ((Long) range.getMax()) >= longKey) {
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
            String[] columns = { "previousHash", "blockDataHash", "transActionsMetaData", "transactionCount",
                    "blockNo", "channelId" };
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
        }
        throw new BlkchnException("Cannot create dataframe from unknown object type");
    }

}
