package com.impetus.fabric.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

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

	public DataFrame select(List<String> cols) {
		List<List<Object>> returnData = new ArrayList<>();
		for(List<Object> record : data) {
			List<Object> returnRec = new ArrayList<>();
			for(String col : cols) {
				int colIndex;
				if(columns.contains(col)) {
					colIndex = columns.indexOf(col);
				} else if(aliasMapping.containsKey(col)) {
					String actualCol = aliasMapping.get(col);
					colIndex = columns.indexOf(actualCol);
				} else {
					throw new RuntimeException("Column " + col + " doesn't exist in table");
				}
				returnRec.add(record.get(colIndex));
			}
			returnData.add(returnRec);
		}
		return new DataFrame(returnData, cols, aliasMapping);
	}
}
