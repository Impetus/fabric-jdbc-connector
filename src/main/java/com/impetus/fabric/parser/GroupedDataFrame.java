package com.impetus.fabric.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.impetus.blkch.sql.query.Column;
import com.impetus.blkch.sql.query.FunctionNode;
import com.impetus.blkch.sql.query.IdentifierNode;
import com.impetus.blkch.sql.query.SelectItem;

public class GroupedDataFrame {

	private List<Integer> groupIndices;
	
	private List<String> columns;
	
	private Map<String, String> aliasMapping;
	
	private Map<List<Object>, List<List<Object>>> groupData;
	
	GroupedDataFrame(List<Integer> groupIndices, List<List<Object>> data, List<String> columns, Map<String, String> aliasMapping) {
		this.groupIndices = groupIndices;
		this.columns = columns;
		this.aliasMapping = aliasMapping;
		this.groupData = data.stream().collect(Collectors.groupingBy(list -> groupIndices.stream().map(index -> list.get(index)).collect(Collectors.toList()), Collectors.toList()));
	}
	
	public DataFrame select(List<SelectItem> cols) {
		List<List<Object>> returnData = new ArrayList<>();
		List<String> returnCols = new ArrayList<>();
		boolean columnsInitialized = false;
		for(Map.Entry<List<Object>, List<List<Object>>> entry : groupData.entrySet()) {
			for(List<Object> record : entry.getValue()) {
				List<Object> returnRec = new ArrayList<>();
				for(SelectItem col : cols) {
					if(col.hasChildType(Column.class)) {
						int colIndex;
						String colName = col.getChildType(Column.class, 0).getChildType(IdentifierNode.class, 0).getValue();
						if(columns.contains(colName)) {
							colIndex = columns.indexOf(colName);
							if(!groupIndices.contains(colIndex)) {
								throw new RuntimeException("Select column " + colName + " should exist in group by clause");
							}
							if (!columnsInitialized) {
								returnCols.add(colName);
							}
						} else if(aliasMapping.containsKey(colName)) {
							String actualCol = aliasMapping.get(colName);
							colIndex = columns.indexOf(actualCol);
							if(!groupIndices.contains(colIndex)) {
								throw new RuntimeException("Select column " + colName + " should exist in group by clause");
							}
							if (!columnsInitialized) {
								returnCols.add(actualCol);
							}
						} else {
							throw new RuntimeException("Column " + colName + " doesn't exist in table");
						}
						returnRec.add(record.get(colIndex));
					} else if(col.hasChildType(FunctionNode.class)) {
						Object computeResult = computeFunction(col.getChildType(FunctionNode.class, 0), entry.getValue());
						returnRec.add(computeResult);
						if(col.hasChildType(IdentifierNode.class)) {
							if (!columnsInitialized) {
								returnCols.add(col.getChildType(IdentifierNode.class, 0).getValue());
							}
						} else if(!columnsInitialized) {
							returnCols.add(createFunctionColName(col.getChildType(FunctionNode.class, 0)));
						}
					}
				}
				returnData.add(returnRec);
				columnsInitialized = true;
			}
		}
		System.out.println(returnCols);
		return new DataFrame(returnData, returnCols, aliasMapping);
	}
	
	private Object computeFunction(FunctionNode function, List<List<Object>> data) {
		String func = function.getChildType(IdentifierNode.class, 0).getValue();
		List<Object> columnData = new ArrayList<>();
		if(function.hasChildType(FunctionNode.class)) {
			columnData.add(computeFunction(function.getChildType(FunctionNode.class, 0), data));
		} else {
			int colIndex;
			String colName = function.getChildType(Column.class, 0).getChildType(IdentifierNode.class, 0).getValue();
			if(columns.contains(colName)) {
				colIndex = columns.indexOf(colName);
			} else if(aliasMapping.containsKey(colName)) {
				String actualCol = aliasMapping.get(colName);
				colIndex = columns.indexOf(actualCol);
			} else {
				throw new RuntimeException("Column " + colName + " doesn't exist in table");
			}
			for(List<Object> record : data) {
				columnData.add(record.get(colIndex));
			}
		}
		switch(func) {
			case "count" : return AggregationFunctions.count(columnData);
			case "sum" : return AggregationFunctions.sum(columnData);
			default: throw new RuntimeException("Unidentified function: " + func);
		}
	}
	
	private String createFunctionColName(FunctionNode function) {
		String func = function.getChildType(IdentifierNode.class, 0).getValue();
		if(function.hasChildType(FunctionNode.class)) {
			return func + "(" + createFunctionColName(function.getChildType(FunctionNode.class, 0)) + ")";
		} else {
			String colName = function.getChildType(Column.class, 0).getChildType(IdentifierNode.class, 0).getValue();
			return func + "(" + colName + ")";
		}
	}
}
