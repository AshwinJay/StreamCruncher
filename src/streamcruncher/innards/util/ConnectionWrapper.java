/*
 * StreamCruncher:  Copyright (c) 2006-2008, Ashwin Jayaprakash. All Rights Reserved.
 * Contact:         ashwin {dot} jayaprakash {at} gmail {dot} com
 * Web:             http://www.StreamCruncher.com
 * 
 * This file is part of StreamCruncher.
 * 
 *     StreamCruncher is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 * 
 *     StreamCruncher is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 * 
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with StreamCruncher. If not, see <http://www.gnu.org/licenses/>.
 */
package streamcruncher.innards.util;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;

/*
 * Author: Ashwin Jayaprakash Date: Jan 5, 2007 Time: 9:25:57 PM
 */

/**
 * Interface methods as of JDK 1.6.
 */
public class ConnectionWrapper implements Connection {
    protected final Connection realConnection;

    public ConnectionWrapper(Connection realConnection) {
        this.realConnection = realConnection;
    }

    public void clearWarnings() throws SQLException {
        realConnection.clearWarnings();
    }

    public void close() throws SQLException {
        realConnection.close();
    }

    public void commit() throws SQLException {
        realConnection.commit();
    }

    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return realConnection.createArrayOf(typeName, elements);
    }

    public Blob createBlob() throws SQLException {
        return realConnection.createBlob();
    }

    public Clob createClob() throws SQLException {
        return realConnection.createClob();
    }

    public NClob createNClob() throws SQLException {
        return realConnection.createNClob();
    }

    public SQLXML createSQLXML() throws SQLException {
        return realConnection.createSQLXML();
    }

    public Statement createStatement() throws SQLException {
        return realConnection.createStatement();
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return realConnection.createStatement(resultSetType, resultSetConcurrency);
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        return realConnection.createStatement(resultSetType, resultSetConcurrency,
                resultSetHoldability);
    }

    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return realConnection.createStruct(typeName, attributes);
    }

    public boolean getAutoCommit() throws SQLException {
        return realConnection.getAutoCommit();
    }

    public String getCatalog() throws SQLException {
        return realConnection.getCatalog();
    }

    public Properties getClientInfo() throws SQLException {
        return realConnection.getClientInfo();
    }

    public String getClientInfo(String name) throws SQLException {
        return realConnection.getClientInfo(name);
    }

    public int getHoldability() throws SQLException {
        return realConnection.getHoldability();
    }

    public DatabaseMetaData getMetaData() throws SQLException {
        return realConnection.getMetaData();
    }

    public int getTransactionIsolation() throws SQLException {
        return realConnection.getTransactionIsolation();
    }

    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return realConnection.getTypeMap();
    }

    public SQLWarning getWarnings() throws SQLException {
        return realConnection.getWarnings();
    }

    public boolean isClosed() throws SQLException {
        return realConnection.isClosed();
    }

    public boolean isReadOnly() throws SQLException {
        return realConnection.isReadOnly();
    }

    public boolean isValid(int timeout) throws SQLException {
        return realConnection.isValid(timeout);
    }

    public String nativeSQL(String sql) throws SQLException {
        return realConnection.nativeSQL(sql);
    }

    public CallableStatement prepareCall(String sql) throws SQLException {
        return realConnection.prepareCall(sql);
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return realConnection.prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        return realConnection.prepareCall(sql, resultSetType, resultSetConcurrency,
                resultSetHoldability);
    }

    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return realConnection.prepareStatement(sql);
    }

    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
            throws SQLException {
        return realConnection.prepareStatement(sql, autoGeneratedKeys);
    }

    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return realConnection.prepareStatement(sql, columnIndexes);
    }

    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return realConnection.prepareStatement(sql, columnNames);
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType,
            int resultSetConcurrency) throws SQLException {
        return realConnection.prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType,
            int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return realConnection.prepareStatement(sql, resultSetType, resultSetConcurrency,
                resultSetHoldability);
    }

    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        realConnection.releaseSavepoint(savepoint);
    }

    public void rollback() throws SQLException {
        realConnection.rollback();
    }

    public void rollback(Savepoint savepoint) throws SQLException {
        realConnection.rollback(savepoint);
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException {
        realConnection.setAutoCommit(autoCommit);
    }

    public void setCatalog(String catalog) throws SQLException {
        realConnection.setCatalog(catalog);
    }

    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        realConnection.setClientInfo(properties);
    }

    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        realConnection.setClientInfo(name, value);
    }

    public void setHoldability(int holdability) throws SQLException {
        realConnection.setHoldability(holdability);
    }

    public void setReadOnly(boolean readOnly) throws SQLException {
        realConnection.setReadOnly(readOnly);
    }

    public Savepoint setSavepoint() throws SQLException {
        return realConnection.setSavepoint();
    }

    public Savepoint setSavepoint(String name) throws SQLException {
        return realConnection.setSavepoint(name);
    }

    public void setTransactionIsolation(int level) throws SQLException {
        realConnection.setTransactionIsolation(level);
    }

    public void setTypeMap(Map<String, Class<?>> arg0) throws SQLException {
        realConnection.setTypeMap(arg0);
    }

    /**
     * todo Not sure how to implement this. {@inheritDoc}
     */
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLException("Unsupported operation");
    }

    /**
     * todo Not sure how to implement this. {@inheritDoc}
     */
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLException("Unsupported operation");
    }
}
