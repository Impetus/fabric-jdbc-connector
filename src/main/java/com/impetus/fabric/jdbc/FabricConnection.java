/*******************************************************************************
* * Copyright 2017 Impetus Infotech.
* *
* * Licensed under the Apache License, Version 2.0 (the "License");
* * you may not use this file except in compliance with the License.
* * You may obtain a copy of the License at
* *
* * http://www.apache.org/licenses/LICENSE-2.0
* *
* * Unless required by applicable law or agreed to in writing, software
* * distributed under the License is distributed on an "AS IS" BASIS,
* * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* * See the License for the specific language governing permissions and
* * limitations under the License.
******************************************************************************/
package com.impetus.fabric.jdbc;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import com.impetus.blkch.jdbc.BlkchnConnection;

public class FabricConnection implements BlkchnConnection {

    private String configPath;

    private String url;

    FabricConnection(String url, Properties props) {
        this.url = url;
        this.configPath = props.getProperty("configPath");
    }

    String getConfigPath() {
        return configPath;
    }

    public boolean isWrapperFor(Class<?> arg0) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public <T> T unwrap(Class<T> arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public void abort(Executor arg0) throws SQLException {
        // TODO Auto-generated method stub

    }

    public void clearWarnings() throws SQLException {
        // TODO Auto-generated method stub

    }

    public void close() throws SQLException {
        // TODO Auto-generated method stub

    }

    public void commit() throws SQLException {
        // TODO Auto-generated method stub

    }

    public Array createArrayOf(String arg0, Object[] arg1) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public Blob createBlob() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public Clob createClob() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public NClob createNClob() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public SQLXML createSQLXML() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public Statement createStatement() throws SQLException {
        return createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }

    public Statement createStatement(int type, int concurrency) throws SQLException {
        return createStatement(type, concurrency, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    public Statement createStatement(int type, int concurrency, int holdability) throws SQLException {
        return new FabricStatement(this, type, concurrency, holdability);
    }

    public Struct createStruct(String arg0, Object[] arg1) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean getAutoCommit() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public String getCatalog() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public Properties getClientInfo() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public String getClientInfo(String arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public int getHoldability() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    public DatabaseMetaData getMetaData() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public int getNetworkTimeout() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    public String getSchema() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public int getTransactionIsolation() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    public Map<String, Class<?>> getTypeMap() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public SQLWarning getWarnings() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean isClosed() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean isReadOnly() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean isValid(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public String nativeSQL(String arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public CallableStatement prepareCall(String arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public CallableStatement prepareCall(String arg0, int arg1, int arg2) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public CallableStatement prepareCall(String arg0, int arg1, int arg2, int arg3) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public PreparedStatement prepareStatement(String arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public PreparedStatement prepareStatement(String arg0, int arg1) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public PreparedStatement prepareStatement(String arg0, int[] arg1) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public PreparedStatement prepareStatement(String arg0, String[] arg1) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public PreparedStatement prepareStatement(String arg0, int arg1, int arg2) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public PreparedStatement prepareStatement(String arg0, int arg1, int arg2, int arg3) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public void releaseSavepoint(Savepoint arg0) throws SQLException {
        // TODO Auto-generated method stub

    }

    public void rollback() throws SQLException {
        // TODO Auto-generated method stub

    }

    public void rollback(Savepoint arg0) throws SQLException {
        // TODO Auto-generated method stub

    }

    public void setAutoCommit(boolean arg0) throws SQLException {
        // TODO Auto-generated method stub

    }

    public void setCatalog(String arg0) throws SQLException {
        // TODO Auto-generated method stub

    }

    public void setClientInfo(Properties arg0) throws SQLClientInfoException {
        // TODO Auto-generated method stub

    }

    public void setClientInfo(String arg0, String arg1) throws SQLClientInfoException {
        // TODO Auto-generated method stub

    }

    public void setHoldability(int arg0) throws SQLException {
        // TODO Auto-generated method stub

    }

    public void setNetworkTimeout(Executor arg0, int arg1) throws SQLException {
        // TODO Auto-generated method stub

    }

    public void setReadOnly(boolean arg0) throws SQLException {
        // TODO Auto-generated method stub

    }

    public Savepoint setSavepoint() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public Savepoint setSavepoint(String arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public void setSchema(String arg0) throws SQLException {
        // TODO Auto-generated method stub

    }

    public void setTransactionIsolation(int arg0) throws SQLException {
        // TODO Auto-generated method stub

    }

    public void setTypeMap(Map<String, Class<?>> arg0) throws SQLException {
        // TODO Auto-generated method stub

    }

}
