package com.impetus.fabric.jdbc.schema;

import com.impetus.blkch.sql.schema.Column;
import com.impetus.blkch.sql.schema.ColumnType;
import com.impetus.blkch.sql.schema.Table;

public class FabricColumn implements Column {
	
	private String name;
	
	private ColumnType type;
	
	private Table table;
	
	public FabricColumn(String name, ColumnType type) {
		this.name = name;
		this.type = type;
	}
	
	void setTable(Table table) {
		this.table = table;
	}

	public String getName() {
		return name;
	}

	public Table getTable() {
		return table;
	}

	public ColumnType getType() {
		return type;
	}

}
