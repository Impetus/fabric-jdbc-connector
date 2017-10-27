package com.impetus.fabric.jdbc.schema;

import java.util.ArrayList;
import java.util.List;

import com.impetus.blkch.sql.schema.Column;
import com.impetus.blkch.sql.schema.Table;

public class FabricTable implements Table {
	
	private String name;
	
	private List<FabricColumn> columns;
	
	public FabricTable(String name, List<FabricColumn> columns) {
		this.name = name;
		this.columns = columns;
		for(FabricColumn column : columns) {
			column.setTable(this);
		}
	}

	public Column getColumnByName(String columnName) {
		for(Column column : columns) {
			if(column.getName().equalsIgnoreCase(columnName)) {
				return column;
			}
		}
		throw new RuntimeException("Column " + columnName + " doesn't exist in table " + name);
	}

	public List<Column> getColumns() {
		List<Column> cols = new ArrayList<Column>();
		for(Column column : columns) {
			cols.add(column);
		}
		return cols;
	}

	public String getName() {
		return name;
	}

	@Override
	public Column getColumn(String name) {
		// TODO Auto-generated method stub
		return null;
	}

}
