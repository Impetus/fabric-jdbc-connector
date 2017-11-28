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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;

import com.impetus.blkch.jdbc.BlkchnDatabaseMetaData;

public class FabricDatabaseMetaData implements BlkchnDatabaseMetaData {

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean allProceduresAreCallable() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean allTablesAreSelectable() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean deletesAreDetected(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean generatedKeyAlwaysReturned() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public ResultSet getAttributes(String arg0, String arg1, String arg2, String arg3) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public ResultSet getBestRowIdentifier(String arg0, String arg1, String arg2, int arg3, boolean arg4)
            throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public String getCatalogSeparator() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public String getCatalogTerm() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public ResultSet getCatalogs() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public ResultSet getClientInfoProperties() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public ResultSet getColumnPrivileges(String arg0, String arg1, String arg2, String arg3) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public ResultSet getColumns(String arg0, String arg1, String arg2, String arg3) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public Connection getConnection() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public ResultSet getCrossReference(String arg0, String arg1, String arg2, String arg3, String arg4, String arg5)
            throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public int getDatabaseMajorVersion() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    public int getDatabaseMinorVersion() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    public String getDatabaseProductName() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public String getDatabaseProductVersion() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public int getDefaultTransactionIsolation() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    public int getDriverMajorVersion() {
        // TODO Auto-generated method stub
        return 0;
    }

    public int getDriverMinorVersion() {
        // TODO Auto-generated method stub
        return 0;
    }

    public String getDriverName() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public String getDriverVersion() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public ResultSet getExportedKeys(String arg0, String arg1, String arg2) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public String getExtraNameCharacters() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public ResultSet getFunctionColumns(String arg0, String arg1, String arg2, String arg3) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public ResultSet getFunctions(String arg0, String arg1, String arg2) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public String getIdentifierQuoteString() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public ResultSet getImportedKeys(String arg0, String arg1, String arg2) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public ResultSet getIndexInfo(String arg0, String arg1, String arg2, boolean arg3, boolean arg4)
            throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public int getJDBCMajorVersion() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    public int getJDBCMinorVersion() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    public int getMaxBinaryLiteralLength() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    public int getMaxCatalogNameLength() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    public int getMaxCharLiteralLength() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    public int getMaxColumnNameLength() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    public int getMaxColumnsInGroupBy() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    public int getMaxColumnsInIndex() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    public int getMaxColumnsInOrderBy() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    public int getMaxColumnsInSelect() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    public int getMaxColumnsInTable() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    public int getMaxConnections() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    public int getMaxCursorNameLength() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    public int getMaxIndexLength() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    public int getMaxProcedureNameLength() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    public int getMaxRowSize() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    public int getMaxSchemaNameLength() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    public int getMaxStatementLength() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    public int getMaxStatements() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    public int getMaxTableNameLength() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    public int getMaxTablesInSelect() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    public int getMaxUserNameLength() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    public String getNumericFunctions() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public ResultSet getPrimaryKeys(String arg0, String arg1, String arg2) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public ResultSet getProcedureColumns(String arg0, String arg1, String arg2, String arg3) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public String getProcedureTerm() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public ResultSet getProcedures(String arg0, String arg1, String arg2) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public ResultSet getPseudoColumns(String arg0, String arg1, String arg2, String arg3) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public int getResultSetHoldability() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    public RowIdLifetime getRowIdLifetime() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public String getSQLKeywords() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public int getSQLStateType() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    public String getSchemaTerm() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public ResultSet getSchemas() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public ResultSet getSchemas(String arg0, String arg1) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public String getSearchStringEscape() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public String getStringFunctions() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public ResultSet getSuperTables(String arg0, String arg1, String arg2) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public ResultSet getSuperTypes(String arg0, String arg1, String arg2) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public String getSystemFunctions() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public ResultSet getTablePrivileges(String arg0, String arg1, String arg2) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public ResultSet getTableTypes() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public ResultSet getTables(String arg0, String arg1, String arg2, String[] arg3) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public String getTimeDateFunctions() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public ResultSet getTypeInfo() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public ResultSet getUDTs(String arg0, String arg1, String arg2, int[] arg3) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public String getURL() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public String getUserName() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public ResultSet getVersionColumns(String arg0, String arg1, String arg2) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean insertsAreDetected(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean isCatalogAtStart() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean isReadOnly() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean locatorsUpdateCopy() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean nullPlusNonNullIsNull() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean nullsAreSortedAtEnd() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean nullsAreSortedAtStart() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean nullsAreSortedHigh() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean nullsAreSortedLow() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean othersDeletesAreVisible(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean othersInsertsAreVisible(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean othersUpdatesAreVisible(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean ownDeletesAreVisible(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean ownInsertsAreVisible(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean ownUpdatesAreVisible(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean storesLowerCaseIdentifiers() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean storesMixedCaseIdentifiers() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean storesUpperCaseIdentifiers() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsANSI92FullSQL() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsBatchUpdates() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsColumnAliasing() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsConvert() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsConvert(int arg0, int arg1) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsCoreSQLGrammar() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsCorrelatedSubqueries() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsExpressionsInOrderBy() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsExtendedSQLGrammar() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsFullOuterJoins() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsGetGeneratedKeys() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsGroupBy() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsGroupByBeyondSelect() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsGroupByUnrelated() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsLikeEscapeClause() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsLimitedOuterJoins() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsMinimumSQLGrammar() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsMultipleOpenResults() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsMultipleResultSets() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsMultipleTransactions() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsNamedParameters() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsNonNullableColumns() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsOrderByUnrelated() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsOuterJoins() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsPositionedDelete() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsPositionedUpdate() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsResultSetConcurrency(int arg0, int arg1) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsResultSetHoldability(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsResultSetType(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsSavepoints() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsSchemasInDataManipulation() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsSelectForUpdate() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsStatementPooling() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsStoredProcedures() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsSubqueriesInComparisons() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsSubqueriesInExists() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsSubqueriesInIns() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsTableCorrelationNames() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsTransactionIsolationLevel(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsTransactions() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsUnion() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsUnionAll() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean updatesAreDetected(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean usesLocalFilePerTable() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean usesLocalFiles() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

}
