package com.impetus.fabric.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.List;

import com.impetus.blkch.jdbc.BlkchnStatement;
import com.impetus.fabric.query.QueryBlock;

public class FabricStatement implements BlkchnStatement {
	
	private FabricConnection connection;
	
	private int type;
	
	private int concurrency;
	
	private int holdablity;
	
	FabricStatement(FabricConnection conn, int type, int concurrency, int holdability) {
		this.connection = conn;
		this.type = type;
		this.concurrency = concurrency;
		this.holdablity = holdability;
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public <T> T unwrap(Class<T> iface) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public void addBatch(String arg0) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void cancel() throws SQLException {
		// TODO Auto-generated method stub

	}

	public void clearBatch() throws SQLException {
		// TODO Auto-generated method stub

	}

	public void clearWarnings() throws SQLException {
		// TODO Auto-generated method stub

	}

	public void close() throws SQLException {
		// TODO Auto-generated method stub

	}

	public void closeOnCompletion() throws SQLException {
		// TODO Auto-generated method stub

	}

	public boolean execute(String arg0) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean execute(String arg0, int arg1) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean execute(String arg0, int[] arg1) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean execute(String arg0, String[] arg1) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public int[] executeBatch() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public ResultSet executeQuery(String query) throws SQLException {
		QueryBlock qb = new QueryBlock(this.connection.getConfigPath());
		qb.enrollAndRegister("Swati Raj");

		//List<Object[]> blockdata = qb.blockchainInfoList();FabricResultSet
		String[] columns = new String[] {"block_num", "block_hash", "prev_block_hash", "channel_name", "transaction_count"};
		System.out.println("Creating result set");
		String result = qb.blockchainInfo();
		System.out.println(result);
		//List<Object[]> fab = qb.GetBlock(8);
		//ResultSet rs = new FabricResultSet(this, fab, columns);

		System.out.println( "Querying The Full BlockChain");
		List<Object[]> fab = qb.GetBlock(-1);
		ResultSet rs = new FabricResultSet(this, fab, columns);

		// Query For Transaction Within a Block
		/*String[] columns = new String[] {"transaction_id", "channel_id", "transaction_status", "transaction_args", "endorser_id","chaincode_name","trans_read_key","trans_write_key"};
		FabricTransaction fab = qb.QueryTransactionWithBlkID(3, "dfd3da947750f35b78654e696f7ee6c5420c9fe0e2a316f31de97e215951d1a1");
		ResultSet rs = new FabricResultSet(this, fab.getRecordData(), columns);*/
		return rs;
	}

	public int executeUpdate(String arg0) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	public int executeUpdate(String arg0, int arg1) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	public int executeUpdate(String arg0, int[] arg1) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	public int executeUpdate(String arg0, String[] arg1) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	public Connection getConnection() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public int getFetchDirection() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	public int getFetchSize() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	public ResultSet getGeneratedKeys() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public int getMaxFieldSize() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	public int getMaxRows() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	public boolean getMoreResults() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean getMoreResults(int arg0) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public int getQueryTimeout() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	public ResultSet getResultSet() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public int getResultSetConcurrency() throws SQLException {
		return concurrency;
	}

	public int getResultSetHoldability() throws SQLException {
		return holdablity;
	}

	public int getResultSetType() throws SQLException {
		return type;
	}

	public int getUpdateCount() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	public SQLWarning getWarnings() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean isCloseOnCompletion() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean isClosed() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean isPoolable() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public void setCursorName(String arg0) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setEscapeProcessing(boolean arg0) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setFetchDirection(int arg0) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setFetchSize(int arg0) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setMaxFieldSize(int arg0) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setMaxRows(int arg0) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setPoolable(boolean arg0) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setQueryTimeout(int arg0) throws SQLException {
		// TODO Auto-generated method stub

	}

}
