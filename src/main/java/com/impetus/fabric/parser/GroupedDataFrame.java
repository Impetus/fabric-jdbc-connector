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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.impetus.blkch.BlkchnException;
import com.impetus.blkch.sql.query.Column;
import com.impetus.blkch.sql.query.Comparator;
import com.impetus.blkch.sql.query.FilterItem;
import com.impetus.blkch.sql.query.FunctionNode;
import com.impetus.blkch.sql.query.HavingClause;
import com.impetus.blkch.sql.query.IdentifierNode;
import com.impetus.blkch.sql.query.LogicalOperation;
import com.impetus.blkch.sql.query.SelectItem;

public class GroupedDataFrame {

    private List<Integer> groupIndices;

    private List<String> columns;

    private Map<String, String> aliasMapping;

    private Map<List<Object>, List<List<Object>>> groupData;

    GroupedDataFrame(List<Integer> groupIndices, List<List<Object>> data, List<String> columns,
            Map<String, String> aliasMapping) {
        this.groupIndices = groupIndices;
        this.columns = columns;
        this.aliasMapping = aliasMapping;
        this.groupData = data.stream().collect(
                Collectors.groupingBy(
                        list -> groupIndices.stream().map(index -> list.get(index)).collect(Collectors.toList()),
                        Collectors.toList()));
    }
    
    private GroupedDataFrame(List<Integer> groupIndices, Map<List<Object>, List<List<Object>>> groupData, List<String> columns,
            Map<String, String> aliasMapping) {
        this.groupIndices = groupIndices;
        this.columns = columns;
        this.aliasMapping = aliasMapping;
        this.groupData = groupData;
    }
    
    // Exposed this getter for test cases. Should be package private.
    Map<List<Object>, List<List<Object>>> getGroupData() {
        return groupData;
    }

    public DataFrame select(List<SelectItem> cols) {
        List<List<Object>> returnData = new ArrayList<>();
        List<String> returnCols = new ArrayList<>();
        boolean columnsInitialized = false;
        for (Map.Entry<List<Object>, List<List<Object>>> entry : groupData.entrySet()) {
            List<Object> returnRec = new ArrayList<>();
            if (entry.getValue().size() == 0) {
                continue;
            }
            for (SelectItem col : cols) {
                if (col.hasChildType(Column.class)) {
                    int colIndex;
                    String colName = col.getChildType(Column.class, 0).getChildType(IdentifierNode.class, 0).getValue();
                    if (columns.contains(colName)) {
                        colIndex = columns.indexOf(colName);
                        if (!groupIndices.contains(colIndex)) {
                            throw new BlkchnException("Select column " + colName + " should exist in group by clause");
                        }
                        if (!columnsInitialized) {
                            returnCols.add(colName);
                        }
                    } else if (aliasMapping.containsKey(colName)) {
                        String actualCol = aliasMapping.get(colName);
                        colIndex = columns.indexOf(actualCol);
                        if (!groupIndices.contains(colIndex)) {
                            throw new BlkchnException("Select column " + colName + " should exist in group by clause");
                        }
                        if (!columnsInitialized) {
                            returnCols.add(actualCol);
                        }
                    } else {
                        throw new BlkchnException("Column " + colName + " doesn't exist in table");
                    }
                    returnRec.add(entry.getValue().get(0).get(colIndex));
                } else if (col.hasChildType(FunctionNode.class)) {
                    Object computeResult = computeFunction(col.getChildType(FunctionNode.class, 0), entry.getValue());
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
    
    public GroupedDataFrame having(HavingClause havingClause) {
        Map<List<Object>, List<List<Object>>> groupData;
        if(havingClause.hasChildType(FilterItem.class)) {
            groupData = executeSingleHavingClause(havingClause.getChildType(FilterItem.class, 0));
        } else {
            groupData = executeMultipleHavingClause(havingClause.getChildType(LogicalOperation.class, 0));
        }
        return new GroupedDataFrame(groupIndices, groupData, columns, aliasMapping);
    }
    
    private Map<List<Object>, List<List<Object>>> executeSingleHavingClause(FilterItem filterItem) {
        String column = filterItem.getChildType(Column.class, 0).getChildType(IdentifierNode.class, 0).getValue();
        Comparator comparator = filterItem.getChildType(Comparator.class, 0);
        String value = filterItem.getChildType(IdentifierNode.class, 0).getValue().replace("'", "");
        int groupIdx = -1;
        boolean invalidFilterCol = false;
        if(columns.contains(column)) {
            if(groupIndices.contains(columns.indexOf(column))) {
                groupIdx = groupIndices.indexOf(columns.indexOf(column));
            } else {
                invalidFilterCol = true;
            }
        } else if(aliasMapping.containsKey(column)) {
            if(groupIndices.contains(columns.indexOf(aliasMapping.get(column)))) {
                groupIdx = groupIndices.indexOf(columns.indexOf(aliasMapping.get(column)));
            } else {
                invalidFilterCol = true;
            }
        } else {
            invalidFilterCol = true;
        }
        if(invalidFilterCol || (groupIdx == -1)) {
            throw new BlkchnException("Column " + column + " must appear in GROUP BY clause");
        }
        final int groupIndex = groupIdx;
        Map<List<Object>, List<List<Object>>> filterData = groupData.entrySet().stream().filter(entry -> {
            List<Object> keys = entry.getKey();
            Object cellValue = keys.get(groupIndex);
            if(cellValue == null) {
                return false;
            }
            if(cellValue instanceof Number) {
                Double cell = Double.parseDouble(cellValue.toString());
                Double doubleValue = Double.parseDouble(value);
                if(comparator.isEQ()) {
                    return cell.equals(doubleValue);
                } else if(comparator.isGT()) {
                    return cell > doubleValue;
                } else if(comparator.isGTE()) {
                    return cell >= doubleValue;
                } else if(comparator.isLT()) {
                    return cell < doubleValue;
                } else if(comparator.isLTE()) {
                    return cell <= doubleValue;
                } else {
                    return !cell.equals(doubleValue);
                }
            } else {
                int comparisionValue = cellValue.toString().compareTo(value);
                if(comparator.isEQ()) {
                    return comparisionValue == 0;
                } else if(comparator.isGT()) {
                    return comparisionValue > 0;
                } else if(comparator.isGTE()) {
                    return comparisionValue >= 0;
                } else if(comparator.isLT()) {
                    return comparisionValue < 0;
                } else if(comparator.isLTE()) {
                    return comparisionValue <= 0;
                } else {
                    return comparisionValue != 0;
                }
            }
        }).collect(Collectors.toMap(p -> p.getKey(), p -> p.getValue()));
        return filterData;
    }
    
    private Map<List<Object>, List<List<Object>>> executeMultipleHavingClause(LogicalOperation operation) {
        if (operation.getChildNodes().size() != 2) {
            throw new BlkchnException("Logical operation should have two boolean expressions");
        }
        Map<List<Object>, List<List<Object>>> firstOut, secondOut, returnMap = new HashMap<>();
        if (operation.getChildNode(0) instanceof LogicalOperation) {
            firstOut = executeMultipleHavingClause((LogicalOperation) operation.getChildNode(0));
        } else {
            FilterItem filterItem = (FilterItem) operation.getChildNode(0);
            firstOut = executeSingleHavingClause(filterItem);
        }
        if (operation.getChildNode(1) instanceof LogicalOperation) {
            secondOut = executeMultipleHavingClause((LogicalOperation) operation.getChildNode(1));
        } else {
            FilterItem filterItem = (FilterItem) operation.getChildNode(1);
            secondOut = executeSingleHavingClause(filterItem);
        }
        if(operation.isAnd()) {
            for(List<Object> key : firstOut.keySet()) {
                if(secondOut.containsKey(key)) {
                    returnMap.put(key, secondOut.get(key));
                }
            }
        } else {
            returnMap.putAll(firstOut);
            for(Map.Entry<List<Object>, List<List<Object>>> entry : secondOut.entrySet()) {
                returnMap.putIfAbsent(entry.getKey(), entry.getValue());
            }
        }
        return returnMap;
    }

    private Object computeFunction(FunctionNode function, List<List<Object>> data) {
        String func = function.getChildType(IdentifierNode.class, 0).getValue();
        List<Object> columnData = new ArrayList<>();
        if (function.hasChildType(FunctionNode.class)) {
            columnData.add(computeFunction(function.getChildType(FunctionNode.class, 0), data));
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
