package com.impetus.fabric.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

public class FabricResultSet implements ResultSet {
	
	private Statement statement;
	
	private String[] columns;
	
	private Object[] recordData;
	
	private List<Object[]> data;
	
	private int recIdx;
	
	private boolean closed;
	
	private static final int BEFORE_FIRST = -1;
	
	FabricResultSet(Statement statement, List<Object[]> data, String[] columns) {
		this.statement = statement;
		this.data = data;
		this.columns = columns;
		this.recIdx = BEFORE_FIRST;
		this.closed = false;
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public <T> T unwrap(Class<T> iface) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean absolute(int arg0) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public void afterLast() throws SQLException {
		// TODO Auto-generated method stub

	}

	public void beforeFirst() throws SQLException {
		// TODO Auto-generated method stub

	}

	public void cancelRowUpdates() throws SQLException {
		// TODO Auto-generated method stub

	}

	public void clearWarnings() throws SQLException {
		// TODO Auto-generated method stub

	}

	public void close() throws SQLException {
		if(!isClosed()) {
			closed = true;
			statement = null;
		}

	}

	public void deleteRow() throws SQLException {
		// TODO Auto-generated method stub

	}

	public int findColumn(String column) throws SQLException {
		for(int i = 0 ; i < columns.length ; i++) {
			if(columns[i].equalsIgnoreCase(column)) {
				return i + 1;
			}
		}
		throw new SQLException(String.format("Result set data doesn't contain column %s", column));
	}

	public boolean first() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public Array getArray(int arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public Array getArray(String arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public InputStream getAsciiStream(int arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public InputStream getAsciiStream(String arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public BigDecimal getBigDecimal(int index) throws SQLException {
		if(index > recordData.length) {
			throw new SQLException(String.format("Result set doesn't contain index %d", index));
		}
		return recordData[index - 1] == null ? new BigDecimal(0.0) : new BigDecimal(recordData[index - 1].toString());
	}

	public BigDecimal getBigDecimal(String column) throws SQLException {
		return getBigDecimal(findColumn(column));
	}

	public BigDecimal getBigDecimal(int index, int scale) throws SQLException {
		return getBigDecimal(index).setScale(scale, BigDecimal.ROUND_HALF_UP);
	}

	public BigDecimal getBigDecimal(String column, int scale) throws SQLException {
		return getBigDecimal(findColumn(column), scale);
	}

	public InputStream getBinaryStream(int arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public InputStream getBinaryStream(String arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public Blob getBlob(int arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public Blob getBlob(String arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean getBoolean(int index) throws SQLException {
		if(index > recordData.length) {
			throw new SQLException(String.format("Result set doesn't contain index %d", index));
		}
		return recordData[index - 1] == null ? false : Boolean.parseBoolean(recordData[index - 1].toString());
	}

	public boolean getBoolean(String column) throws SQLException {
		return getBoolean(findColumn(column));
	}

	public byte getByte(int index) throws SQLException {
		if(index > recordData.length) {
			throw new SQLException(String.format("Result set doesn't contain index %d", index));
		}
		return recordData[index - 1] == null ? 0 : Byte.parseByte(recordData[index - 1].toString());
	}

	public byte getByte(String column) throws SQLException {
		return getByte(findColumn(column));
	}

	public byte[] getBytes(int arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public byte[] getBytes(String arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public Reader getCharacterStream(int arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public Reader getCharacterStream(String arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public Clob getClob(int arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public Clob getClob(String arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public int getConcurrency() throws SQLException {
		return statement.getResultSetConcurrency();
	}

	public String getCursorName() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public Date getDate(int index) throws SQLException {
		if(index > recordData.length) {
			throw new SQLException(String.format("Result set doesn't contain index %d", index));
		}
		DateFormat format = DateFormat.getDateInstance();
		try {
			return recordData[index - 1] == null ? null : new java.sql.Date(format.parse(recordData[index - 1].toString()).getTime());
		} catch (ParseException e) {
			throw new SQLException(e.getMessage());
		}
	}

	public Date getDate(String column) throws SQLException {
		return getDate(findColumn(column));
	}

	public Date getDate(int arg0, Calendar arg1) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public Date getDate(String arg0, Calendar arg1) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public double getDouble(int index) throws SQLException {
		if(index > recordData.length) {
			throw new SQLException(String.format("Result set doesn't contain index %d", index));
		}
		return recordData[index - 1] == null ? 0.0 : Double.parseDouble(recordData[index - 1].toString());
	}

	public double getDouble(String column) throws SQLException {
		return getDouble(findColumn(column));
	}

	public int getFetchDirection() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	public int getFetchSize() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	public float getFloat(int index) throws SQLException {
		if(index > recordData.length) {
			throw new SQLException(String.format("Result set doesn't contain index %d", index));
		}
		return recordData[index - 1] == null ? 0.0f : Float.parseFloat(recordData[index - 1].toString());
	}

	public float getFloat(String column) throws SQLException {
		return getFloat(findColumn(column));
	}

	public int getHoldability() throws SQLException {
		return statement.getResultSetHoldability();
	}

	public int getInt(int index) throws SQLException {
		if(index > recordData.length) {
			throw new SQLException(String.format("Result set doesn't contain index %d", index));
		}
		return recordData[index - 1] == null ? 0 : Integer.parseInt(recordData[index - 1].toString());
	}

	public int getInt(String column) throws SQLException {
		return getInt(findColumn(column));
	}

	public long getLong(int index) throws SQLException {
		if(index > recordData.length) {
			throw new SQLException(String.format("Result set doesn't contain index %d", index));
		}
		return recordData[index - 1] == null ? 0 : Long.parseLong(recordData[index - 1].toString());
	}

	public long getLong(String column) throws SQLException {
		return getLong(findColumn(column));
	}

	public ResultSetMetaData getMetaData() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public Reader getNCharacterStream(int arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public Reader getNCharacterStream(String arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public NClob getNClob(int arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public NClob getNClob(String arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public String getNString(int arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public String getNString(String arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public Object getObject(int index) throws SQLException {
		if(index > recordData.length) {
			throw new SQLException(String.format("Result set doesn't contain index %d", index));
		}
		return recordData[index -1];
	}

	public Object getObject(String column) throws SQLException {
		return getObject(findColumn(column));
	}

	public Object getObject(int arg0, Map<String, Class<?>> arg1) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public Object getObject(String arg0, Map<String, Class<?>> arg1) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public <T> T getObject(int arg0, Class<T> arg1) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public <T> T getObject(String arg0, Class<T> arg1) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public Ref getRef(int arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public Ref getRef(String arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public int getRow() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	public RowId getRowId(int arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public RowId getRowId(String arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public SQLXML getSQLXML(int arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public SQLXML getSQLXML(String arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public short getShort(int index) throws SQLException {
		if(index > recordData.length) {
			throw new SQLException(String.format("Result set doesn't contain index %d", index));
		}
		return recordData[index -1] == null ? 0 : Short.parseShort(recordData[index - 1].toString());
	}

	public short getShort(String column) throws SQLException {
		return getShort(findColumn(column));
	}

	public Statement getStatement() throws SQLException {
		return statement;
	}

	public String getString(int index) throws SQLException {
		if(index > recordData.length) {
			throw new SQLException(String.format("Result set doesn't contain index %d", index));
		}
		return recordData[index - 1] == null ? null : recordData[index - 1].toString();
	}

	public String getString(String column) throws SQLException {
		return getString(findColumn(column));
	}

	public Time getTime(int arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public Time getTime(String arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public Time getTime(int arg0, Calendar arg1) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public Time getTime(String arg0, Calendar arg1) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public Timestamp getTimestamp(int arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public Timestamp getTimestamp(String arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public Timestamp getTimestamp(int arg0, Calendar arg1) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public Timestamp getTimestamp(String arg0, Calendar arg1) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public int getType() throws SQLException {
		return statement.getResultSetType();
	}

	public URL getURL(int index) throws SQLException {
		String url = getString(index);
        try {
            return url == null ? null : new java.net.URL(url);
        } catch (MalformedURLException e) {
            throw new SQLException(e.getMessage());
        }
	}

	public URL getURL(String column) throws SQLException {
		return getURL(findColumn(column));
	}

	public InputStream getUnicodeStream(int arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public InputStream getUnicodeStream(String arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public SQLWarning getWarnings() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public void insertRow() throws SQLException {
		// TODO Auto-generated method stub

	}

	public boolean isAfterLast() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean isBeforeFirst() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean isClosed() throws SQLException {
		return closed;
	}

	public boolean isFirst() throws SQLException {
		return recIdx == 0;
	}

	public boolean isLast() throws SQLException {
		return recIdx == data.size() - 1;
	}

	public boolean last() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public void moveToCurrentRow() throws SQLException {
		// TODO Auto-generated method stub

	}

	public void moveToInsertRow() throws SQLException {
		// TODO Auto-generated method stub

	}

	public boolean next() throws SQLException {
		if(++recIdx >= data.size()) {
			return false;
		}
		recordData = data.get(recIdx);
		return true;
	}

	public boolean previous() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public void refreshRow() throws SQLException {
		// TODO Auto-generated method stub

	}

	public boolean relative(int arg0) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean rowDeleted() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean rowInserted() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean rowUpdated() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public void setFetchDirection(int arg0) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setFetchSize(int arg0) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateArray(int arg0, Array arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateArray(String arg0, Array arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateAsciiStream(int arg0, InputStream arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateAsciiStream(String arg0, InputStream arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateAsciiStream(int arg0, InputStream arg1, int arg2) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateAsciiStream(String arg0, InputStream arg1, int arg2) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateAsciiStream(int arg0, InputStream arg1, long arg2) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateAsciiStream(String arg0, InputStream arg1, long arg2) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateBigDecimal(int arg0, BigDecimal arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateBigDecimal(String arg0, BigDecimal arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateBinaryStream(int arg0, InputStream arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateBinaryStream(String arg0, InputStream arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateBinaryStream(int arg0, InputStream arg1, int arg2) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateBinaryStream(String arg0, InputStream arg1, int arg2) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateBinaryStream(int arg0, InputStream arg1, long arg2) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateBinaryStream(String arg0, InputStream arg1, long arg2) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateBlob(int arg0, Blob arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateBlob(String arg0, Blob arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateBlob(int arg0, InputStream arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateBlob(String arg0, InputStream arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateBlob(int arg0, InputStream arg1, long arg2) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateBlob(String arg0, InputStream arg1, long arg2) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateBoolean(int arg0, boolean arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateBoolean(String arg0, boolean arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateByte(int arg0, byte arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateByte(String arg0, byte arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateBytes(int arg0, byte[] arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateBytes(String arg0, byte[] arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateCharacterStream(int arg0, Reader arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateCharacterStream(String arg0, Reader arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateCharacterStream(int arg0, Reader arg1, int arg2) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateCharacterStream(String arg0, Reader arg1, int arg2) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateCharacterStream(int arg0, Reader arg1, long arg2) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateCharacterStream(String arg0, Reader arg1, long arg2) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateClob(int arg0, Clob arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateClob(String arg0, Clob arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateClob(int arg0, Reader arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateClob(String arg0, Reader arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateClob(int arg0, Reader arg1, long arg2) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateClob(String arg0, Reader arg1, long arg2) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateDate(int arg0, Date arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateDate(String arg0, Date arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateDouble(int arg0, double arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateDouble(String arg0, double arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateFloat(int arg0, float arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateFloat(String arg0, float arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateInt(int arg0, int arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateInt(String arg0, int arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateLong(int arg0, long arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateLong(String arg0, long arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateNCharacterStream(int arg0, Reader arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateNCharacterStream(String arg0, Reader arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateNCharacterStream(int arg0, Reader arg1, long arg2) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateNCharacterStream(String arg0, Reader arg1, long arg2) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateNClob(int arg0, NClob arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateNClob(String arg0, NClob arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateNClob(int arg0, Reader arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateNClob(String arg0, Reader arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateNClob(int arg0, Reader arg1, long arg2) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateNClob(String arg0, Reader arg1, long arg2) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateNString(int arg0, String arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateNString(String arg0, String arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateNull(int arg0) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateNull(String arg0) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateObject(int arg0, Object arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateObject(String arg0, Object arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateObject(int arg0, Object arg1, int arg2) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateObject(String arg0, Object arg1, int arg2) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateRef(int arg0, Ref arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateRef(String arg0, Ref arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateRow() throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateRowId(int arg0, RowId arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateRowId(String arg0, RowId arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateSQLXML(int arg0, SQLXML arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateSQLXML(String arg0, SQLXML arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateShort(int arg0, short arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateShort(String arg0, short arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateString(int arg0, String arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateString(String arg0, String arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateTime(int arg0, Time arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateTime(String arg0, Time arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateTimestamp(int arg0, Timestamp arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void updateTimestamp(String arg0, Timestamp arg1) throws SQLException {
		// TODO Auto-generated method stub

	}

	public boolean wasNull() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

}
