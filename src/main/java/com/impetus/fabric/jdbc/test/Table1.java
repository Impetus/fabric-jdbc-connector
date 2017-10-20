package com.impetus.fabric.jdbc.test;

public class Table1 implements DataTable {

	public int col1;
	
	public String col2;
	
	public double col3;
	
	public Table1(int col1, String col2, double col3) {
		this.col1 = col1;
		this.col2 = col2;
		this.col3 = col3;
	}
}
