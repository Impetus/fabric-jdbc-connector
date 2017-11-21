package com.impetus.fabric.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.impetus.blkch.sql.query.Column;
import com.impetus.blkch.sql.query.IdentifierNode;
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
		for(List<Object> record : data) {
			List<Object> returnRec = new ArrayList<>();
			for(SelectItem col : cols) {
				if(col.hasChildType(StarNode.class)) {
					for(String colName : columns) {
						int colIndex = columns.indexOf(colName);
						returnRec.add(record.get(colIndex));
						returnCols.add(colName);
					}
				} else if(col.hasChildType(Column.class)) {
					int colIndex;
					String colName = col.getChildType(Column.class, 0).getChildType(IdentifierNode.class, 0).getValue();
					if(columns.contains(colName)) {
						colIndex = columns.indexOf(colName);
						returnCols.add(colName);
					} else if(aliasMapping.containsKey(colName)) {
						String actualCol = aliasMapping.get(colName);
						colIndex = columns.indexOf(actualCol);
						returnCols.add(actualCol);
					} else {
						throw new RuntimeException("Column " + colName + " doesn't exist in table");
					}
					returnRec.add(record.get(colIndex));
				}
			}
			returnData.add(returnRec);
		}
		return new DataFrame(returnData, returnCols, aliasMapping);
	}
}
