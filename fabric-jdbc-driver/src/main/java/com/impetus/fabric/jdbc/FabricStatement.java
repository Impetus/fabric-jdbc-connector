/*******************************************************************************
 * * Copyright 2018 Impetus Infotech.
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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.impetus.blkch.sql.query.Column;
import com.impetus.blkch.sql.query.FunctionNode;
import com.impetus.blkch.sql.query.SelectItem;
import com.impetus.blkch.sql.query.StarNode;
import com.impetus.blkch.util.Utilities;
import com.impetus.fabric.parser.FabricPhysicalPlan;
import com.impetus.fabric.query.FabricTables;
import org.antlr.v4.runtime.CommonTokenStream;

import com.impetus.blkch.BlkchnErrorListener;
import com.impetus.blkch.jdbc.BlkchnStatement;
import com.impetus.blkch.sql.DataFrame;
import com.impetus.blkch.sql.asset.Asset;
import com.impetus.blkch.sql.function.CallFunction;
import com.impetus.blkch.sql.generated.BlkchnSqlLexer;
import com.impetus.blkch.sql.generated.BlkchnSqlParser;
import com.impetus.blkch.sql.parser.AbstractSyntaxTreeVisitor;
import com.impetus.blkch.sql.parser.BlockchainVisitor;
import com.impetus.blkch.sql.parser.CaseInsensitiveCharStream;
import com.impetus.blkch.sql.parser.LogicalPlan;
import com.impetus.blkch.sql.query.FromItem;
import com.impetus.blkch.sql.query.IdentifierNode;
import com.impetus.blkch.sql.query.RangeNode;
import com.impetus.blkch.sql.query.Table;
import com.impetus.fabric.parser.CAManager;
import com.impetus.fabric.parser.FabricAssetManager;
import com.impetus.fabric.parser.FunctionExecutor;
import com.impetus.fabric.parser.InsertExecutor;
import com.impetus.fabric.parser.QueryExecutor;
import com.impetus.fabric.query.QueryBlock;

public class FabricStatement implements BlkchnStatement {

    private FabricConnection connection;

    private int type;

    private int concurrency;

    private int holdablity;

    private FabricResultSet resultSet;
    
    private RangeNode<?> pageRange;

    FabricStatement(FabricConnection conn, int type, int concurrency, int holdability) {
        this.connection = conn;
        this.type = type;
        this.concurrency = concurrency;
        this.holdablity = holdability;
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void addBatch(String sql) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void cancel() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void clearBatch() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void clearWarnings() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void close() throws SQLException {
        // Nothing to do
    }

    public void closeOnCompletion() throws SQLException {
        // Nothing to do
    }

    public boolean execute(String sql) throws SQLException {
        LogicalPlan logicalPlan = getLogicalPlan(sql);
        QueryBlock queryBlock = this.connection.getQueryObject();
        switch (logicalPlan.getType()) {
            case CREATE_FUNCTION:
                new FunctionExecutor(logicalPlan, queryBlock).executeCreate();
                return false;

            case CALL_FUNCTION:
                executeQuery(sql);
                return true;

            case QUERY:
                executeQuery(sql);
                return true;

            case INSERT:
                new InsertExecutor(logicalPlan, queryBlock).executeInsert();
                return false;

            case CREATE_ASSET:
                new FabricAssetManager(logicalPlan, queryBlock.getConf()).executeCreateAsset();
                return false;

            case DELETE_FUNCTION:
                new FunctionExecutor(logicalPlan, queryBlock).executeCall();
                return false;

            case DROP_ASSET:
                new FabricAssetManager(logicalPlan, queryBlock.getConf()).executeDropAsset();
                return false;
                
            case UPGRADE_FUNCTION:
                new FunctionExecutor(logicalPlan, queryBlock).executeUpgrade();
                return false;
                
            case CREATE_USER:
                new CAManager(logicalPlan, queryBlock).registerUser();
                return false;

            default:
                return false;
        }
    }

    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean execute(String sql, String[] columnNames) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int[] executeBatch() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public ResultSet executeQuery(String query) throws SQLException {
        LogicalPlan logicalPlan = getLogicalPlan(query);
        QueryBlock queryBlock = this.connection.getQueryObject();
        DataFrame dataframe = null;
        String tableName;
        switch (logicalPlan.getType()) {
            case CALL_FUNCTION:
                CallFunction callFunc = logicalPlan.getCallFunction();
                if (!callFunc.hasChildType(Asset.class)) {
                    tableName = null;
                } else {
                    tableName = callFunc.getChildType(Asset.class, 0).getChildType(IdentifierNode.class, 0)
                            .getValue();
                }
                dataframe = new FunctionExecutor(logicalPlan, queryBlock).executeCall();
                break;

            default:
                Table table = logicalPlan.getQuery().getChildType(FromItem.class, 0).getChildType(Table.class, 0);
                tableName = table.getChildType(IdentifierNode.class, 0).getValue();
                QueryExecutor executor = new QueryExecutor(logicalPlan, queryBlock);
                if(this.pageRange != null) {
                    executor.paginate(pageRange);
                }
                dataframe = executor.executeQuery();
        }
        if(dataframe.isEmpty()){
            DataFrame dfWithSchema = getDataFrameWithSchema(tableName,logicalPlan);
            resultSet = new FabricResultSet(this, dfWithSchema, tableName);
        }
        else {
            resultSet = new FabricResultSet(this, dataframe, tableName);
        }
        return resultSet;
    }

    private DataFrame getDataFrameWithSchema(String tableName, LogicalPlan logicalPlan){

        FabricPhysicalPlan physicalPlan = new FabricPhysicalPlan(logicalPlan);
        List<SelectItem> cols = physicalPlan.getSelectItems();
        Map<String, Integer> columnNamesMap = buildColumnNamesMap(getColumnNames(logicalPlan));
        Map<String, String> aliasMapping = physicalPlan.getColumnAliasMapping();

        List<String> returnCols = new ArrayList<>();
        for (SelectItem col : cols) {
            if (col.hasChildType(StarNode.class)) {
                for (String colName : columnNamesMap.keySet()) {
                        returnCols.add(colName);
                }

            } else if (col.hasChildType(Column.class)) {
                String colName = col.getChildType(Column.class, 0).getChildType(IdentifierNode.class, 0).getValue();
                if (columnNamesMap.get(colName) != null) {
                        returnCols.add(colName);
                } else if (aliasMapping.containsKey(colName)) {
                    String actualCol = aliasMapping.get(colName);
                        returnCols.add(colName);
                } else {
                    throw new RuntimeException("Column " + colName + " doesn't exist in table");
                }
            } else if (col.hasChildType(FunctionNode.class)) {
                    returnCols.add(Utilities.createFunctionColName(col.getChildType(FunctionNode.class, 0)));
            }
        }
        return new DataFrame(new ArrayList<>(), returnCols, aliasMapping);
    }

    private Map<String, Integer> buildColumnNamesMap(List<String> columns) {
        Map<String, Integer> columnsMap = new LinkedHashMap<>();
        int index = 0;
        for (String col : columns) {
            columnsMap.put(col, index++);
        }
        return columnsMap;
    }

    private List<String> getColumnNames(LogicalPlan logicalPlan){
        Table table = logicalPlan.getQuery().getChildType(FromItem.class, 0).getChildType(Table.class, 0);
        String tableName = table.getChildType(IdentifierNode.class, 0).getValue();
        List<String> columnList;
        switch (tableName) {
            case FabricTables.BLOCK:
                columnList = FabricPhysicalPlan.getFabricTableColumnMap().get(FabricTables.BLOCK);
                break;
            case FabricTables.TRANSACTION:
                columnList = FabricPhysicalPlan.getFabricTableColumnMap().get(FabricTables.TRANSACTION);
                break;
            case FabricTables.TRANSACTION_ACTION:
                columnList = FabricPhysicalPlan.getFabricTableColumnMap().get(FabricTables.TRANSACTION_ACTION);
                break;
            case FabricTables.READ_WRITE_SET:
                columnList = FabricPhysicalPlan.getFabricTableColumnMap().get(FabricTables.READ_WRITE_SET);
                break;
            default:
                columnList = new ArrayList<>();
        }
        return columnList;
    }


    private LogicalPlan getLogicalPlan(String query) {
        BlkchnSqlLexer lexer = new BlkchnSqlLexer(new CaseInsensitiveCharStream(query));
        lexer.removeErrorListeners();
        lexer.addErrorListener(BlkchnErrorListener.INSTANCE);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        BlkchnSqlParser parser = new BlkchnSqlParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(BlkchnErrorListener.INSTANCE);
        AbstractSyntaxTreeVisitor visitor = new BlockchainVisitor();
        return visitor.visitSingleStatement(parser.singleStatement());
    }

    public int executeUpdate(String sql) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Connection getConnection() throws SQLException {
        return connection;
    }

    public int getFetchDirection() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getFetchSize() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public ResultSet getGeneratedKeys() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getMaxFieldSize() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getMaxRows() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean getMoreResults() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean getMoreResults(int current) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getQueryTimeout() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public ResultSet getResultSet() throws SQLException {
        return resultSet;
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
        throw new UnsupportedOperationException();
    }

    public SQLWarning getWarnings() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isCloseOnCompletion() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isClosed() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isPoolable() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setCursorName(String name) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setEscapeProcessing(boolean enable) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setFetchDirection(int direction) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setFetchSize(int rows) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setMaxFieldSize(int max) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setMaxRows(int max) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setPoolable(boolean poolable) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setQueryTimeout(int seconds) throws SQLException {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void setPageRange(RangeNode<?> pageRange) {
        this.pageRange = pageRange;
    }

    @Override
    public Number getBlockHeight() {
        return connection.getQueryObject().getChannelHeight();
    }

    @Override
    public RangeNode getProbableRange(String sql) {
        LogicalPlan logicalPlan = getLogicalPlan(sql);
        QueryBlock queryBlock = this.connection.getQueryObject();
        QueryExecutor executor = new QueryExecutor(logicalPlan, queryBlock);
        return executor.getProbableRange();
    }

}
