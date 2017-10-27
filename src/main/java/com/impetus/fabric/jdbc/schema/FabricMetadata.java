package com.impetus.fabric.jdbc.schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.impetus.blkch.sql.schema.ColumnType;
import com.impetus.blkch.sql.schema.Schema;
import com.impetus.blkch.sql.schema.Table;

public class FabricMetadata implements Schema {
	
	private List<Table> tables = new ArrayList<Table>();

	public FabricMetadata() {
		Table table1 = new FabricTable("table1", Arrays.asList(new FabricColumn("col1", ColumnType.INT), new FabricColumn("col2", ColumnType.STRING), new FabricColumn("col3", ColumnType.DOUBLE)));
		Table table2 = new FabricTable("table2", Arrays.asList(new FabricColumn("col4", ColumnType.INT), new FabricColumn("col5", ColumnType.STRING)));
		tables.add(table1);
		tables.add(table2);
		
	}

	public Table getTable(String tableName) {
		for(Table table : tables) {
			if(table.getName().equalsIgnoreCase(tableName)) {
				return table;
			}
		}
		throw new RuntimeException("Table " + tableName + " doesn't exist");
	}

	@Override
	public List<Table> getTables() {
		// TODO Auto-generated method stub
		return null;
	}

}
