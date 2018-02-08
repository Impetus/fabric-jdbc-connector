/*******************************************************************************
* * Copyright 2017 Impetus Infotech.
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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.impetus.blkch.BlkchnException;
import com.impetus.blkch.sql.query.Column;
import com.impetus.blkch.sql.query.FunctionNode;
import com.impetus.blkch.sql.query.IdentifierNode;
import com.impetus.blkch.sql.query.LimitClause;
import com.impetus.blkch.sql.query.OrderItem;
import com.impetus.blkch.sql.query.OrderingDirection;
import com.impetus.blkch.sql.query.SelectItem;
import com.impetus.blkch.sql.query.StarNode;

public class DataFrame {

    private List<String> columns;

    private Map<String, String> aliasMapping;

    private List<List<Object>> data;

    DataFrame(List<List<Object>> data, String[] columns, Map<String, String> aliasMapping) {
        this(data, Arrays.asList(columns), aliasMapping);
    }

    DataFrame(List<List<Object>> data, List<String> columns, Map<String, String> aliasMapping) {
        this.columns = columns;
        this.aliasMapping = aliasMapping;
        this.data = data;
    }

    public List<String> getColumns() {
        return columns;
    }

    public Map<String, String> getAliasMapping() {
        return aliasMapping;
    }

    public List<List<Object>> getData() {
        return data;
    }

    public DataFrame select(List<SelectItem> cols) {
        List<List<Object>> returnData = new ArrayList<>();
        List<String> returnCols = new ArrayList<>();
        boolean columnsInitialized = false;
        for (List<Object> record : data) {
            List<Object> returnRec = new ArrayList<>();
            for (SelectItem col : cols) {
                if (col.hasChildType(StarNode.class)) {
                    for (String colName : columns) {
                        int colIndex = columns.indexOf(colName);
                        returnRec.add(record.get(colIndex));
                        if (!columnsInitialized) {
                            returnCols.add(colName);
                        }
                    }
                } else if (col.hasChildType(Column.class)) {
                    int colIndex;
                    String colName = col.getChildType(Column.class, 0).getChildType(IdentifierNode.class, 0).getValue();
                    if (columns.contains(colName)) {
                        colIndex = columns.indexOf(colName);
                        if (!columnsInitialized) {
                            returnCols.add(colName);
                        }
                    } else if (aliasMapping.containsKey(colName)) {
                        String actualCol = aliasMapping.get(colName);
                        colIndex = columns.indexOf(actualCol);
                        if (!columnsInitialized) {
                            returnCols.add(colName);
                        }
                    } else {
                        throw new BlkchnException("Column " + colName + " doesn't exist in table");
                    }
                    returnRec.add(record.get(colIndex));
                } else if (col.hasChildType(FunctionNode.class)) {
                    Object computeResult = computeFunction(col.getChildType(FunctionNode.class, 0));
                    returnRec.add(computeResult);
                    if (col.hasChildType(IdentifierNode.class)) {
                        if (!columnsInitialized) {
                            returnCols.add(col.getChildType(IdentifierNode.class, 0).getValue());
                        }
                    } else if (!columnsInitialized) {
                        returnCols.add(createFunctionColName(col.getChildType(FunctionNode.class, 0)));
                    }
                }
            }
            returnData.add(returnRec);
            columnsInitialized = true;
        }
        return new DataFrame(returnData, returnCols, aliasMapping);
    }

    public DataFrame order(List<OrderItem> orderItems) {
        Map<Integer, OrderingDirection> order = new LinkedHashMap<>();
        for (OrderItem orderItem : orderItems) {
            OrderingDirection direction = orderItem.getChildType(OrderingDirection.class, 0);
            String col = orderItem.getChildType(Column.class, 0).getChildType(IdentifierNode.class, 0).getValue();
            if (columns.contains(col)) {
                order.put(columns.indexOf(col), direction);
            } else if (aliasMapping.containsKey(col)) {
                String actualCol = aliasMapping.get(col);
                order.put(columns.indexOf(actualCol), direction);
            } else {
                throw new BlkchnException("Column " + col + " doesn't exist in table");
            }
        }
        List<List<Object>> sortData = data.stream()
                .map(list -> list.stream().map(obj -> obj).collect(Collectors.toList())).collect(Collectors.toList());
        Collections.sort(sortData, new Comparator<List<Object>>() {

            @Override
            public int compare(List<Object> first, List<Object> second) {
                for (Map.Entry<Integer, OrderingDirection> entry : order.entrySet()) {
                    int colIndex = entry.getKey();
                    Object firstObject = first.get(colIndex);
                    Object secondObject = second.get(colIndex);
                    if (firstObject.equals(secondObject)) {
                        continue;
                    }
                    OrderingDirection direction = entry.getValue();
                    int diff;
                    if (firstObject instanceof Integer) {
                        diff = (((Integer) firstObject) - ((Integer) secondObject)) < 0 ? -1 : +1;
                    } else if (firstObject instanceof Long) {
                        diff = (((Long) firstObject) - ((Long) secondObject)) < 0 ? -1 : +1;
                    } else if (firstObject instanceof Double) {
                        diff = (((Double) firstObject) - ((Double) secondObject)) < 0.0 ? -1 : +1;
                    } else if (firstObject instanceof Date) {
                        diff = (((Date) firstObject).getTime() - ((Date) secondObject).getTime()) < 0.0 ? -1 : +1;
                    } else {
                        diff = firstObject.toString().compareTo(secondObject.toString());
                    }
                    return direction.isAsc() ? diff : diff * -1;
                }
                return 0;
            }

        });
        return new DataFrame(sortData, columns, aliasMapping);
    }

    public DataFrame limit(LimitClause limitClause) {
        String limitValue = limitClause.getChildType(IdentifierNode.class, 0).getValue();
        int limit;
        try {
            limit = Integer.parseInt(limitValue);
        } catch (NumberFormatException e) {
            throw new BlkchnException(e);
        }
        if (limit < 0) {
            throw new BlkchnException("limit value should not be less than zero");
        }
        List<List<Object>> limitedData = data.stream().limit(limit).collect(Collectors.toList());
        return new DataFrame(limitedData, columns, aliasMapping);
    }

    public GroupedDataFrame group(List<String> groupCols) {
        List<Integer> groupIndices = new ArrayList<>();
        for (String colName : groupCols) {
            if (columns.contains(colName)) {
                groupIndices.add(columns.indexOf(colName));
            } else if (aliasMapping.containsKey(colName)) {
                String actualCol = aliasMapping.get(colName);
                groupIndices.add(columns.indexOf(actualCol));
            } else {
                throw new BlkchnException("Column " + colName + " doesn't exist in table");
            }
        }
        return new GroupedDataFrame(groupIndices, data, columns, aliasMapping);
    }

    public void show() {
        columns.stream().forEach(col -> System.out.print(col + " "));
        System.out.println();
        data.stream().forEach(list -> {
            list.stream().forEach(cell -> System.out.print(cell + " "));
            System.out.println();
        });
    }

    private Object computeFunction(FunctionNode function) {
        String func = function.getChildType(IdentifierNode.class, 0).getValue();
        List<Object> columnData = new ArrayList<>();
        if (function.hasChildType(FunctionNode.class)) {
            columnData.add(computeFunction(function.getChildType(FunctionNode.class, 0)));
        } else {
            int colIndex;
            String colName = function.getChildType(Column.class, 0).getChildType(IdentifierNode.class, 0).getValue();
            if (columns.contains(colName)) {
                colIndex = columns.indexOf(colName);
            } else if (aliasMapping.containsKey(colName)) {
                String actualCol = aliasMapping.get(colName);
                colIndex = columns.indexOf(actualCol);
            } else {
                throw new BlkchnException("Column " + colName + " doesn't exist in table");
            }
            for (List<Object> record : data) {
                columnData.add(record.get(colIndex));
            }
        }
        switch (func) {
            case "count":
                return AggregationFunctions.count(columnData);
            case "sum":
                return AggregationFunctions.sum(columnData);
            default:
                throw new BlkchnException("Unidentified function: " + func);
        }
    }

    private String createFunctionColName(FunctionNode function) {
        String func = function.getChildType(IdentifierNode.class, 0).getValue();
        if (function.hasChildType(FunctionNode.class)) {
            return func + "(" + createFunctionColName(function.getChildType(FunctionNode.class, 0)) + ")";
        } else {
            String colName = function.getChildType(Column.class, 0).getChildType(IdentifierNode.class, 0).getValue();
            return func + "(" + colName + ")";
        }
    }

}
