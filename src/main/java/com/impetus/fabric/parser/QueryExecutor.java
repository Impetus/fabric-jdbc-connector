package com.impetus.fabric.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.hyperledger.fabric.sdk.BlockInfo;
import org.hyperledger.fabric.sdk.Channel;
import org.hyperledger.fabric.sdk.exception.InvalidArgumentException;
import org.hyperledger.fabric.sdk.exception.ProposalException;

import com.google.protobuf.InvalidProtocolBufferException;
import com.impetus.blkch.BlkchnException;
import com.impetus.blkch.sql.parser.AbstractQueryExecutor;
import com.impetus.blkch.sql.parser.LogicalPlan;
import com.impetus.blkch.sql.parser.PhysicalPlan;
import com.impetus.blkch.sql.parser.PhysicalPlan.Color;
import com.impetus.blkch.sql.parser.TreeNode;
import com.impetus.blkch.sql.query.Column;
import com.impetus.blkch.sql.query.Comparator;
import com.impetus.blkch.sql.query.DataNode;
import com.impetus.blkch.sql.query.DirectAPINode;
import com.impetus.blkch.sql.query.FilterItem;
import com.impetus.blkch.sql.query.FromItem;
import com.impetus.blkch.sql.query.IdentifierNode;
import com.impetus.blkch.sql.query.LogicalOperation;
import com.impetus.blkch.sql.query.LogicalOperation.Operator;
import com.impetus.blkch.sql.query.RangeNode;
import com.impetus.blkch.sql.query.Table;
import com.impetus.blkch.util.Range;
import com.impetus.blkch.util.RangeOperations;
import com.impetus.fabric.query.QueryBlock;

public class QueryExecutor extends AbstractQueryExecutor {

    private LogicalPlan logicalPlan;

    private QueryBlock queryBlock;

    private PhysicalPlan physicalPlan;

    private Map<String, Object> dataMap = new HashMap<>();

    private Map<String, Map<String, Object>> auxillaryDataMap = new HashMap<>();

    public QueryExecutor(LogicalPlan logicalPlan, QueryBlock queryBlock) {
        this.logicalPlan = logicalPlan;
        this.queryBlock = queryBlock;
        this.physicalPlan = new FabricPhysicalPlan(logicalPlan);
    }

    public void executeQuery() {
        if (!physicalPlan.validateLogicalPlan()) {
            throw new BlkchnException("This query can't be executed");
        }
        // TODO Auto-generated method stub

    }

    private void getFromTable() {
        Table table = logicalPlan.getQuery().getChildType(FromItem.class, 0).getChildType(Table.class, 0);
        String tableName = table.getChildType(IdentifierNode.class, 0).getValue();
        if (physicalPlan.getWhereClause() != null) {
            if (physicalPlan.getWhereClause().hasChildType(LogicalOperation.class)) {
                TreeNode directAPIOptimizedTree = executeDirectAPIs(tableName, physicalPlan.getWhereClause()
                        .getChildType(LogicalOperation.class, 0));
                TreeNode optimizedTree = optimize(directAPIOptimizedTree);
                DataNode<?> finalData = execute(optimizedTree);
            } else {
                // TODO
            }
        } else {
            throw new BlkchnException(
                    "Can't query on hyperledger fabric network without where clause. Data will be huge");
        }

    }

    private TreeNode executeDirectAPIs(String table, TreeNode node) {
        if (node instanceof LogicalOperation) {
            LogicalOperation oper = (LogicalOperation) node;
            TreeNode first = oper.getChildNode(0);
            TreeNode second = oper.getChildNode(1);
            LogicalOperation returnOp = new LogicalOperation(oper.isAnd() ? Operator.AND : Operator.OR);
            returnOp.addChildNode(first);
            returnOp.addChildNode(second);
            return returnOp;
        } else if (node instanceof DirectAPINode) {
            DirectAPINode directAPI = (DirectAPINode) node;
            String column = directAPI.getColumn();
            String value = directAPI.getValue();
            return getDataNode(table, column, value);
        } else if (node instanceof RangeNode<?>) {
            RangeNode<?> rangeNode = (RangeNode<?>) node;
            if (rangeNode.getRangeList().getRanges().size() == 1
                    && rangeNode.getRangeList().getRanges().get(0).getMin() == rangeNode.getRangeList().getRanges()
                            .get(0).getMax()) {
                String column = rangeNode.getColumn();
                String value = rangeNode.getRangeList().getRanges().get(0).getMin().toString();
                return getDataNode(table, column, value);
            }
            return node;

        } else {
            return node;
        }
    }

    private DataNode<?> getDataNode(String table, String column, String value) {
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
    private <T extends Number & Comparable<T>> DataNode<T> executeRangeNode(RangeNode<T> rangeNode) {
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
            T current = range.getMin() == rangeOps.getMinValue() ? (T) new Long(0l) : range.getMin();
            T max = range.getMax() == rangeOps.getMaxValue() ? (T) height : range.getMax();
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
            } while (max.compareTo(rangeOps.add(current, 1)) < 0);
            return new DataNode<>(rangeTable, keys);
        }).collect(Collectors.toList());
        DataNode<T> finalDataNode = dataNodes.get(0);
        if (dataNodes.size() > 1) {
            for (int i = 1; i < dataNodes.size() - 1; i++) {
                finalDataNode = mergeDataNodes(finalDataNode, dataNodes.get(i), Operator.OR);
            }
        }
        return finalDataNode;
    }

    private <T> TreeNode optimize(TreeNode node) {
        if (!(node instanceof LogicalOperation)) {
            return node;
        }
        LogicalOperation oper = (LogicalOperation) node;
        TreeNode left = optimize(oper.getChildNode(0));
        TreeNode right = optimize(oper.getChildNode(1));
        if (oper.isOr()) {
            return optimizeOr(left, right);
        } else {
            return optimizeAnd(left, right);
        }
    }

    @SuppressWarnings("unchecked")
    private <T> TreeNode optimizeAnd(TreeNode left, TreeNode right) {
        if (left instanceof DataNode<?>) {
            DataNode<T> dataNode = (DataNode<T>) left;
            if (right instanceof LogicalOperation) {
                LogicalOperation newOper = new LogicalOperation(Operator.AND);
                newOper.addChildNode(left);
                newOper.addChildNode(right);
                return newOper;
            } else if (right instanceof RangeNode<?>) {
                RangeNode<?> rangeNode = (RangeNode<?>) right;
                return combineRangeAndDataNodes(rangeNode, dataNode, new LogicalOperation(Operator.AND));
            } else if (right instanceof FilterItem) {
                FilterItem filterItem = (FilterItem) right;
                return combineFilterItemAndDataNodes(filterItem, dataNode);
            } else {
                // check for table name if joins are implemented
                DataNode<T> rightDataNode = (DataNode<T>) right;
                return mergeDataNodes(dataNode, rightDataNode, Operator.AND);
            }
        } else if (right instanceof DataNode<?>) {
            DataNode<T> dataNode = (DataNode<T>) right;
            if (left instanceof LogicalOperation) {
                LogicalOperation newOper = new LogicalOperation(Operator.AND);
                newOper.addChildNode(left);
                newOper.addChildNode(right);
                return newOper;
            } else if (left instanceof RangeNode<?>) {
                RangeNode<?> rangeNode = (RangeNode<?>) left;
                return combineRangeAndDataNodes(rangeNode, dataNode, new LogicalOperation(Operator.AND));
            } else {
                FilterItem filterItem = (FilterItem) left;
                return combineFilterItemAndDataNodes(filterItem, dataNode);
            }
        } else if (left instanceof RangeNode<?> && right instanceof RangeNode<?>) {
            RangeNode<?> leftRangeNode = (RangeNode<?>) left;
            RangeNode<?> rightRangeNode = (RangeNode<?>) right;
            if (leftRangeNode.getColumn().equals(rightRangeNode.getColumn())
                    && leftRangeNode.getTable().equals(rightRangeNode.getTable())) {
                RangeOperations<?> rangeOps = physicalPlan.getRangeOperations(leftRangeNode.getTable(),
                        leftRangeNode.getColumn());
                return rangeOps.processRangeNodes(leftRangeNode, rightRangeNode, new LogicalOperation(Operator.AND));
            } else {
                LogicalOperation newOper = new LogicalOperation(Operator.AND);
                newOper.addChildNode(left);
                newOper.addChildNode(right);
                return newOper;
            }
        } else {
            LogicalOperation newOper = new LogicalOperation(Operator.AND);
            newOper.addChildNode(left);
            newOper.addChildNode(right);
            return newOper;
        }
    }

    @SuppressWarnings("unchecked")
    private <T> TreeNode optimizeOr(TreeNode left, TreeNode right) {
        if (left instanceof DataNode<?>) {
            DataNode<T> dataNode = (DataNode<T>) left;
            if (right instanceof LogicalOperation || right instanceof FilterItem) {
                LogicalOperation newOper = new LogicalOperation(Operator.OR);
                newOper.addChildNode(left);
                newOper.addChildNode(right);
                return newOper;
            } else if (right instanceof RangeNode<?>) {
                RangeNode<?> rangeNode = (RangeNode<?>) right;
                return combineRangeAndDataNodes(rangeNode, dataNode, new LogicalOperation(Operator.OR));
            } else {
                // check for table name if joins are implemented
                DataNode<T> rightDataNode = (DataNode<T>) right;
                return mergeDataNodes(dataNode, rightDataNode, Operator.OR);
            }
        } else if (right instanceof DataNode<?>) {
            DataNode<T> dataNode = (DataNode<T>) right;
            if (left instanceof LogicalOperation || left instanceof FilterItem) {
                LogicalOperation newOper = new LogicalOperation(Operator.OR);
                newOper.addChildNode(left);
                newOper.addChildNode(right);
                return newOper;
            } else if (left instanceof RangeNode<?>) {
                RangeNode<?> rangeNode = (RangeNode<?>) left;
                return combineRangeAndDataNodes(rangeNode, dataNode, new LogicalOperation(Operator.OR));
            } else {
                // check for table name if joins are implemented
                DataNode<T> leftDataNode = (DataNode<T>) left;
                return mergeDataNodes(leftDataNode, dataNode, Operator.OR);
            }
        } else if (left instanceof RangeNode<?> && right instanceof RangeNode<?>) {
            RangeNode<?> leftRangeNode = (RangeNode<?>) left;
            RangeNode<?> rightRangeNode = (RangeNode<?>) right;
            if (leftRangeNode.getColumn().equals(rightRangeNode.getColumn())
                    && leftRangeNode.getTable().equals(rightRangeNode.getTable())) {
                RangeOperations<?> rangeOps = physicalPlan.getRangeOperations(leftRangeNode.getTable(),
                        leftRangeNode.getColumn());
                return rangeOps.processRangeNodes(leftRangeNode, rightRangeNode, new LogicalOperation(Operator.OR));
            } else {
                LogicalOperation newOper = new LogicalOperation(Operator.OR);
                newOper.addChildNode(left);
                newOper.addChildNode(right);
                return newOper;
            }
        } else {
            LogicalOperation newOper = new LogicalOperation(Operator.OR);
            newOper.addChildNode(left);
            newOper.addChildNode(right);
            return newOper;
        }
    }

    @SuppressWarnings("unchecked")
    private <T extends Number & Comparable<T>> RangeNode<T> combineRangeAndDataNodes(RangeNode<T> rangeNode,
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
                    rangeNode.getRangeList().addRange(new Range<T>(blockNo, blockNo));
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

    private <T> DataNode<T> combineFilterItemAndDataNodes(FilterItem filterItem, DataNode<T> dataNode) {
        String filterColName = filterItem.getChildType(Column.class, 0).getChildType(IdentifierNode.class, 0)
                .getValue();
        String filterValue = filterItem.getChildType(IdentifierNode.class, 0).getValue();
        Comparator comparator = filterItem.getChildType(Comparator.class, 0);
        List<T> filterKeys = dataNode.getKeys().stream().filter(key -> {
            Object obj = dataMap.get(key.toString());
            return filterField(filterColName, obj, filterValue, comparator);
        }).collect(Collectors.toList());
        return new DataNode<>(dataNode.getTable(), filterKeys);
    }

    private boolean filterField(String fieldName, Object obj, String value, Comparator comparator) {
        boolean retValue = false;
        if (obj instanceof BlockInfo) {
            BlockInfo blockInfo = (BlockInfo) obj;
            switch (fieldName) {
                case "blockDataHash":
                    if (!comparator.isEQ()) {
                        throw new BlkchnException(String.format(
                                "String values in %s field can only be compared for equivalence", fieldName));
                    }
                    retValue = Hex.encodeHexString(blockInfo.getDataHash()).equals(value);
                    break;

                case "transActionsMetaData":
                    if (!comparator.isEQ()) {
                        throw new BlkchnException(String.format(
                                "String values in %s field can only be compared for equivalence", fieldName));
                    }
                    retValue = Hex.encodeHexString(blockInfo.getTransActionsMetaData()).equals(value);
                    break;

                case "transactionCount":
                    Number transactionCount = blockInfo.getEnvelopeCount();
                    retValue = compareNumbers(transactionCount, Integer.parseInt(value), comparator);
                    break;

                case "channelId":
                    try {
                        retValue = blockInfo.getChannelId().equals(value);
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

    private boolean compareNumbers(Number first, Number second, Comparator comparator) {
        if (comparator.isEQ()) {
            return first.doubleValue() == second.doubleValue();
        } else if (comparator.isGT()) {
            return first.doubleValue() > second.doubleValue();
        } else if (comparator.isGTE()) {
            return first.doubleValue() >= second.doubleValue();
        } else if (comparator.isLT()) {
            return first.doubleValue() < second.doubleValue();
        } else if (comparator.isLTE()) {
            return first.doubleValue() <= second.doubleValue();
        } else {
            return first.doubleValue() != second.doubleValue();
        }
    }

    private <T> DataNode<T> mergeDataNodes(DataNode<T> first, DataNode<T> second, Operator op) {
        List<T> newKeys = new ArrayList<>();
        if (op == Operator.AND) {
            for (T firstKey : first.getKeys()) {
                for (T secondKey : second.getKeys()) {
                    if (firstKey.equals(secondKey)) {
                        newKeys.add(secondKey);
                        break;
                    }
                }
            }
        } else {
            newKeys.addAll(first.getKeys());
            for (T key : second.getKeys()) {
                if (!newKeys.contains(key)) {
                    newKeys.add(key);
                }
            }
        }
        return new DataNode<>(first.getTable(), newKeys);
    }

    @SuppressWarnings("unchecked")
    private <T> DataNode<T> execute(TreeNode node) {
        if (node instanceof LogicalOperation) {
            LogicalOperation oper = (LogicalOperation) node;
            if (physicalPlan.validateNode(oper.getChildNode(0)) == Color.GREEN) {
                DataNode<T> first = execute(oper.getChildNode(0));
                if (physicalPlan.validateNode(oper.getChildNode(1)) == Color.GREEN && oper.isOr()) {
                    DataNode<T> second = execute(oper.getChildNode(1));
                    return mergeDataNodes(first, second, Operator.OR);
                } else {
                    return filterWithValue(oper.getChildNode(1), first);
                }
            } else {
                DataNode<T> second = execute(oper.getChildNode(1));
                return filterWithValue(oper.getChildNode(0), second);
            }
        } else if (node instanceof DataNode<?>) {
            return (DataNode<T>) node;
        } else if (node instanceof RangeNode<?>) {
            return (DataNode<T>) executeRangeNode((RangeNode<?>) node);
        }
        throw new BlkchnException("can not execute for node: " + node);
    }

    @SuppressWarnings("unchecked")
    private <T> DataNode<T> filterWithValue(TreeNode node, DataNode<T> dataNode) {
        if (node instanceof LogicalOperation) {
            LogicalOperation oper = (LogicalOperation) node;
            DataNode<T> first = filterWithValue(oper.getChildNode(0), dataNode);
            DataNode<T> second = filterWithValue(oper.getChildNode(1), dataNode);
            if(oper.isAnd()) {
                return mergeDataNodes(first, second, Operator.AND);
            } else {
                return mergeDataNodes(first, second, Operator.OR);
            }
        } else if (node instanceof DataNode<?>) {
            return mergeDataNodes(dataNode, (DataNode<T>) node, Operator.AND);
        } else if (node instanceof RangeNode<?>) {
            return filterRangeNodeWithValue((RangeNode<?>) node, dataNode);
        } else {
            return combineFilterItemAndDataNodes((FilterItem) node, dataNode);
        }
    }

    private <T> DataNode<T> filterRangeNodeWithValue(RangeNode<?> rangeNode, DataNode<T> dataNode) {
        List<T> filteredKeys = dataNode.getKeys().stream().filter(
            key -> {
                if ("block".equals(dataNode.getTable()) && "blockNo".equals(rangeNode.getColumn())) {
                    boolean include = false;
                    Long longKey = (Long) key;
                    for(Range<?> range : rangeNode.getRangeList().getRanges()) {
                        if(((Long) range.getMin()) <= longKey && ((Long) range.getMax()) >= longKey) {
                            include = true;
                            break;
                        }
                    }
                    return include;
                }
                return false;
            }
        ).collect(Collectors.toList());
        return new DataNode<>(dataNode.getTable(), filteredKeys);
    }

}
