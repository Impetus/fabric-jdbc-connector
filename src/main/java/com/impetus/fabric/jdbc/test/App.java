package com.impetus.fabric.jdbc.test;

import java.util.List;

import com.impetus.blkch.sql.query.FromItem;
import com.impetus.blkch.sql.query.SelectItem;
import com.impetus.blkch.sql.schema.MetadataManager;
import com.impetus.fabric.jdbc.schema.FabricMetadata;

public class App {

	public static void main(String[] args) {
		MetadataManager.setMetadata(new FabricMetadata());
		test1();
	}
	
	private static void test1() {
		FromItem fromItem = new FromItem(MetadataManager.getMetadata().getTable("table1"));
		SelectItem selectItem = new SelectItem(fromItem.getTable().getColumn("col2"));
		DataBase db = new DataBase();
		List<Object> data = db.getData(fromItem, selectItem);
		System.out.println(data);
	}

}
