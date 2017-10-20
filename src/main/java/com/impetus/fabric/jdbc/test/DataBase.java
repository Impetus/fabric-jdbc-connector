package com.impetus.fabric.jdbc.test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import com.impetus.blkch.sql.query.FromItem;
import com.impetus.blkch.sql.query.SelectItem;

public class DataBase {

	private List<Table1> table1 = new ArrayList<Table1>();
	
	private List<Table2> table2 = new ArrayList<Table2>();
	
	public DataBase() {
		for(int i = 0 ; i < 5 ; i++) {
			table1.add(new Table1(i, "name" + i, 2.0 * i));
		}
		for(int i = 0 ; i < 10 ; i++) {
			table2.add(new Table2(i * 3, "address" + i));
		}
	}
	
	private List<? extends DataTable> getDataTable(FromItem fi) {
		String tableName = fi.getTable().getName();
		if("table1".equalsIgnoreCase(tableName)) {
			return table1;
		} else {
			return table2;
		}
	}
	
	private List<Object> getColumnData(List<? extends DataTable> tableData, SelectItem si) {
		String colName = si.getColumn().getName();
		List<Object> data = new ArrayList<Object>();
		for(DataTable table : tableData) {
			Field[] columns = table.getClass().getFields();
			for(Field column : columns) {
				if(column.getName().equalsIgnoreCase(colName)) {
					try {
						data.add(column.get(table));
					} catch (IllegalArgumentException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IllegalAccessException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}
		return data;
	}
	
	public List<Object> getData(FromItem fi, SelectItem si) {
		return getColumnData(getDataTable(fi), si);
	}
}
