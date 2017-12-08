package com.impetus.fabric.parser;

import com.impetus.blkch.sql.query.Column;
import com.impetus.blkch.sql.query.Comparator;
import com.impetus.blkch.sql.query.FilterItem;
import com.impetus.blkch.sql.query.FunctionNode;
import com.impetus.blkch.sql.query.IdentifierNode;
import com.impetus.blkch.sql.query.LimitClause;
import com.impetus.blkch.sql.query.LogicalOperation;
import com.impetus.blkch.sql.query.LogicalOperation.Operator;
import com.impetus.blkch.sql.query.OrderItem;
import com.impetus.blkch.sql.query.OrderingDirection;
import com.impetus.blkch.sql.query.SelectItem;
import com.impetus.blkch.sql.query.StarNode;
import com.impetus.blkch.sql.query.Comparator.ComparisionOperator;
import com.impetus.blkch.sql.query.OrderingDirection.Direction;

public class UtilityClass {

    public static SelectItem createColSelectItem(String colName) {
        SelectItem selectItem = new SelectItem();
        Column column = createColumn(colName);
        selectItem.addChildNode(column);
        return selectItem;
    }

    public static SelectItem createStarSelectItem() {
        SelectItem selectItem = new SelectItem();
        selectItem.addChildNode(new StarNode());
        return selectItem;
    }

    public static FunctionNode createAggregateFunction(String func, String colName) {
        FunctionNode function = new FunctionNode();
        function.addChildNode(new IdentifierNode(func));
        Column column = createColumn(colName);
        function.addChildNode(column);
        return function;
    }

    public static OrderItem createOrderItem(String colName, Direction direction) {
        OrderItem orderItem = new OrderItem();
        orderItem.addChildNode(new OrderingDirection(direction));
        Column column = createColumn(colName);
        orderItem.addChildNode(column);
        return orderItem;
    }

    public static LimitClause createLimitClause(String limit) {
        LimitClause limitClause = new LimitClause();
        limitClause.addChildNode(new IdentifierNode(limit));
        return limitClause;
    }
    
    public static Column createColumn(String colName) {
        Column column = new Column();
        IdentifierNode identifierNode = new IdentifierNode(colName);
        column.addChildNode(identifierNode);
        return column;
    }
    
    public static Comparator createComparator(ComparisionOperator op) {
        Comparator comparator = new Comparator(op);
        IdentifierNode identifierNode = new IdentifierNode(op.toString());
        comparator.addChildNode(identifierNode);
        return comparator;
    }
    
    public static FilterItem createFilterItem(String colName, ComparisionOperator op, String value) {
        FilterItem filterItem = new FilterItem();
        filterItem.addChildNode(createColumn(colName));
        filterItem.addChildNode(createComparator(op));
        filterItem.addChildNode(new IdentifierNode(value));
        return filterItem;
    }
    
    public static LogicalOperation createLogicalOperation(FilterItem fi1, FilterItem fi2, Operator op) {
        LogicalOperation logicalOperation = new LogicalOperation(op);
        logicalOperation.addChildNode(fi1);
        logicalOperation.addChildNode(fi2);
        return logicalOperation;
    }
    
    public static LogicalOperation createLogicalOperation(LogicalOperation lo, FilterItem fi, Operator op) {
        LogicalOperation logicalOperation = new LogicalOperation(op);
        logicalOperation.addChildNode(lo);
        logicalOperation.addChildNode(fi);
        return logicalOperation;
    }
}
