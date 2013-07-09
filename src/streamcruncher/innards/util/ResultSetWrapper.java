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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
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
import java.util.Calendar;
import java.util.Map;

/*
 * Author: Ashwin Jayaprakash Date: Jan 6, 2007 Time: 10:26:28 AM
 */
/**
 * Interface methods as of JDK 1.6.
 */
public abstract class ResultSetWrapper implements ResultSet {
    protected final Statement wrappedStatement;

    protected final ResultSet realResultSet;

    protected ResultSetWrapper(Statement wrappedStatement, ResultSet realResultSet) {
        this.wrappedStatement = wrappedStatement;
        this.realResultSet = realResultSet;
    }

    public boolean absolute(int row) throws SQLException {
        return realResultSet.absolute(row);
    }

    public void afterLast() throws SQLException {
        realResultSet.afterLast();
    }

    public void beforeFirst() throws SQLException {
        realResultSet.beforeFirst();
    }

    public void cancelRowUpdates() throws SQLException {
        realResultSet.cancelRowUpdates();
    }

    public void clearWarnings() throws SQLException {
        realResultSet.clearWarnings();
    }

    public void close() throws SQLException {
        realResultSet.close();
    }

    public void deleteRow() throws SQLException {
        realResultSet.deleteRow();
    }

    public int findColumn(String columnLabel) throws SQLException {
        return findColumn(columnLabel);
    }

    public boolean first() throws SQLException {
        return realResultSet.first();
    }

    public Array getArray(int parameterIndex) throws SQLException {
        return this.realResultSet.getArray(parameterIndex);
    }

    public Array getArray(String parameterName) throws SQLException {
        return this.realResultSet.getArray(parameterName);
    }

    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        return realResultSet.getAsciiStream(columnIndex);
    }

    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return realResultSet.getAsciiStream(columnLabel);
    }

    public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
        return this.realResultSet.getBigDecimal(parameterIndex);
    }

    /**
     * {@inheritDoc}
     * 
     * @deprecated
     */
    public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
        return this.realResultSet.getBigDecimal(parameterIndex, scale);
    }

    public BigDecimal getBigDecimal(String parameterName) throws SQLException {
        return this.realResultSet.getBigDecimal(parameterName);
    }

    /**
     * {@inheritDoc}
     * 
     * @deprecated
     */
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return realResultSet.getBigDecimal(columnLabel, scale);
    }

    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        return realResultSet.getBinaryStream(columnIndex);
    }

    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return realResultSet.getBinaryStream(columnLabel);
    }

    public Blob getBlob(int parameterIndex) throws SQLException {
        return this.realResultSet.getBlob(parameterIndex);
    }

    public Blob getBlob(String parameterName) throws SQLException {
        return this.realResultSet.getBlob(parameterName);
    }

    public boolean getBoolean(int parameterIndex) throws SQLException {
        return this.realResultSet.getBoolean(parameterIndex);
    }

    public boolean getBoolean(String parameterName) throws SQLException {
        return this.realResultSet.getBoolean(parameterName);
    }

    public byte getByte(int parameterIndex) throws SQLException {
        return this.realResultSet.getByte(parameterIndex);
    }

    public byte getByte(String parameterName) throws SQLException {
        return this.realResultSet.getByte(parameterName);
    }

    public byte[] getBytes(int parameterIndex) throws SQLException {
        return this.realResultSet.getBytes(parameterIndex);
    }

    public byte[] getBytes(String parameterName) throws SQLException {
        return this.realResultSet.getBytes(parameterName);
    }

    public Reader getCharacterStream(int parameterIndex) throws SQLException {
        return this.realResultSet.getCharacterStream(parameterIndex);
    }

    public Reader getCharacterStream(String parameterName) throws SQLException {
        return this.realResultSet.getCharacterStream(parameterName);
    }

    public Clob getClob(int parameterIndex) throws SQLException {
        return this.realResultSet.getClob(parameterIndex);
    }

    public Clob getClob(String parameterName) throws SQLException {
        return this.realResultSet.getClob(parameterName);
    }

    public int getConcurrency() throws SQLException {
        return realResultSet.getConcurrency();
    }

    public String getCursorName() throws SQLException {
        return realResultSet.getCursorName();
    }

    public Date getDate(int parameterIndex) throws SQLException {
        return this.realResultSet.getDate(parameterIndex);
    }

    public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
        return this.realResultSet.getDate(parameterIndex, cal);
    }

    public Date getDate(String parameterName) throws SQLException {
        return this.realResultSet.getDate(parameterName);
    }

    public Date getDate(String parameterName, Calendar cal) throws SQLException {
        return this.realResultSet.getDate(parameterName, cal);
    }

    public double getDouble(int parameterIndex) throws SQLException {
        return this.realResultSet.getDouble(parameterIndex);
    }

    public double getDouble(String parameterName) throws SQLException {
        return this.realResultSet.getDouble(parameterName);
    }

    public int getFetchDirection() throws SQLException {
        return realResultSet.getFetchDirection();
    }

    public int getFetchSize() throws SQLException {
        return realResultSet.getFetchSize();
    }

    public float getFloat(int parameterIndex) throws SQLException {
        return this.realResultSet.getFloat(parameterIndex);
    }

    public float getFloat(String parameterName) throws SQLException {
        return this.realResultSet.getFloat(parameterName);
    }

    public int getHoldability() throws SQLException {
        return realResultSet.getHoldability();
    }

    public int getInt(int parameterIndex) throws SQLException {
        return this.realResultSet.getInt(parameterIndex);
    }

    public int getInt(String parameterName) throws SQLException {
        return this.realResultSet.getInt(parameterName);
    }

    public long getLong(int parameterIndex) throws SQLException {
        return this.realResultSet.getLong(parameterIndex);
    }

    public long getLong(String parameterName) throws SQLException {
        return this.realResultSet.getLong(parameterName);
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        return realResultSet.getMetaData();
    }

    public Reader getNCharacterStream(int parameterIndex) throws SQLException {
        return this.realResultSet.getNCharacterStream(parameterIndex);
    }

    public Reader getNCharacterStream(String parameterName) throws SQLException {
        return this.realResultSet.getNCharacterStream(parameterName);
    }

    public NClob getNClob(int parameterIndex) throws SQLException {
        return this.realResultSet.getNClob(parameterIndex);
    }

    public NClob getNClob(String parameterName) throws SQLException {
        return this.realResultSet.getNClob(parameterName);
    }

    public String getNString(int parameterIndex) throws SQLException {
        return this.realResultSet.getNString(parameterIndex);
    }

    public String getNString(String parameterName) throws SQLException {
        return this.realResultSet.getNString(parameterName);
    }

    public Object getObject(int parameterIndex) throws SQLException {
        return this.realResultSet.getObject(parameterIndex);
    }

    public Object getObject(int arg0, Map<String, Class<?>> arg1) throws SQLException {
        return this.realResultSet.getObject(arg0, arg1);
    }

    public Object getObject(String parameterName) throws SQLException {
        return this.realResultSet.getObject(parameterName);
    }

    public Object getObject(String arg0, Map<String, Class<?>> arg1) throws SQLException {
        return this.realResultSet.getObject(arg0, arg1);
    }

    public Ref getRef(int parameterIndex) throws SQLException {
        return this.realResultSet.getRef(parameterIndex);
    }

    public Ref getRef(String parameterName) throws SQLException {
        return this.realResultSet.getRef(parameterName);
    }

    public int getRow() throws SQLException {
        return realResultSet.getRow();
    }

    public RowId getRowId(int parameterIndex) throws SQLException {
        return this.realResultSet.getRowId(parameterIndex);
    }

    public RowId getRowId(String parameterName) throws SQLException {
        return this.realResultSet.getRowId(parameterName);
    }

    public short getShort(int parameterIndex) throws SQLException {
        return this.realResultSet.getShort(parameterIndex);
    }

    public short getShort(String parameterName) throws SQLException {
        return this.realResultSet.getShort(parameterName);
    }

    public SQLXML getSQLXML(int parameterIndex) throws SQLException {
        return this.realResultSet.getSQLXML(parameterIndex);
    }

    public SQLXML getSQLXML(String parameterName) throws SQLException {
        return this.realResultSet.getSQLXML(parameterName);
    }

    public Statement getStatement() throws SQLException {
        return wrappedStatement;
    }

    public String getString(int parameterIndex) throws SQLException {
        return this.realResultSet.getString(parameterIndex);
    }

    public String getString(String parameterName) throws SQLException {
        return this.realResultSet.getString(parameterName);
    }

    public Time getTime(int parameterIndex) throws SQLException {
        return this.realResultSet.getTime(parameterIndex);
    }

    public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
        return this.realResultSet.getTime(parameterIndex, cal);
    }

    public Time getTime(String parameterName) throws SQLException {
        return this.realResultSet.getTime(parameterName);
    }

    public Time getTime(String parameterName, Calendar cal) throws SQLException {
        return this.realResultSet.getTime(parameterName, cal);
    }

    public Timestamp getTimestamp(int parameterIndex) throws SQLException {
        return this.realResultSet.getTimestamp(parameterIndex);
    }

    public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
        return this.realResultSet.getTimestamp(parameterIndex, cal);
    }

    public Timestamp getTimestamp(String parameterName) throws SQLException {
        return this.realResultSet.getTimestamp(parameterName);
    }

    public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
        return this.realResultSet.getTimestamp(parameterName, cal);
    }

    public int getType() throws SQLException {
        return realResultSet.getType();
    }

    /**
     * {@inheritDoc}
     * 
     * @deprecated
     */
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        return realResultSet.getUnicodeStream(columnIndex);
    }

    /**
     * {@inheritDoc}
     * 
     * @deprecated
     */
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return realResultSet.getUnicodeStream(columnLabel);
    }

    public URL getURL(int parameterIndex) throws SQLException {
        return this.realResultSet.getURL(parameterIndex);
    }

    public URL getURL(String parameterName) throws SQLException {
        return this.realResultSet.getURL(parameterName);
    }

    public SQLWarning getWarnings() throws SQLException {
        return realResultSet.getWarnings();
    }

    public void insertRow() throws SQLException {
        realResultSet.insertRow();
    }

    public boolean isAfterLast() throws SQLException {
        return realResultSet.isAfterLast();
    }

    public boolean isBeforeFirst() throws SQLException {
        return realResultSet.isBeforeFirst();
    }

    public boolean isClosed() throws SQLException {
        return realResultSet.isClosed();
    }

    public boolean isFirst() throws SQLException {
        return realResultSet.isFirst();
    }

    public boolean isLast() throws SQLException {
        return realResultSet.isLast();
    }

    public boolean last() throws SQLException {
        return realResultSet.last();
    }

    public void moveToCurrentRow() throws SQLException {
        realResultSet.moveToCurrentRow();
    }

    public void moveToInsertRow() throws SQLException {
        realResultSet.moveToInsertRow();
    }

    public boolean next() throws SQLException {
        return realResultSet.next();
    }

    public boolean previous() throws SQLException {
        return realResultSet.previous();
    }

    public void refreshRow() throws SQLException {
        realResultSet.refreshRow();
    }

    public boolean relative(int rows) throws SQLException {
        return realResultSet.relative(rows);
    }

    public boolean rowDeleted() throws SQLException {
        return realResultSet.rowDeleted();
    }

    public boolean rowInserted() throws SQLException {
        return realResultSet.rowInserted();
    }

    public boolean rowUpdated() throws SQLException {
        return realResultSet.rowUpdated();
    }

    public void setFetchDirection(int direction) throws SQLException {
        realResultSet.setFetchDirection(direction);
    }

    public void setFetchSize(int rows) throws SQLException {
        realResultSet.setFetchSize(rows);
    }

    public void updateArray(int columnIndex, Array x) throws SQLException {
        realResultSet.updateArray(columnIndex, x);
    }

    public void updateArray(String columnLabel, Array x) throws SQLException {
        realResultSet.updateArray(columnLabel, x);
    }

    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        realResultSet.updateAsciiStream(columnIndex, x);
    }

    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        realResultSet.updateAsciiStream(columnIndex, x, length);
    }

    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        realResultSet.updateAsciiStream(columnIndex, x, length);
    }

    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        realResultSet.updateAsciiStream(columnLabel, x);
    }

    public void updateAsciiStream(String columnLabel, InputStream x, int length)
            throws SQLException {
        realResultSet.updateAsciiStream(columnLabel, x, length);
    }

    public void updateAsciiStream(String columnLabel, InputStream x, long length)
            throws SQLException {
        realResultSet.updateAsciiStream(columnLabel, x, length);
    }

    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        realResultSet.updateBigDecimal(columnIndex, x);
    }

    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        realResultSet.updateBigDecimal(columnLabel, x);
    }

    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        realResultSet.updateBinaryStream(columnIndex, x);
    }

    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        realResultSet.updateBinaryStream(columnIndex, x, length);
    }

    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        realResultSet.updateBinaryStream(columnIndex, x, length);
    }

    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        realResultSet.updateBinaryStream(columnLabel, x);
    }

    public void updateBinaryStream(String columnLabel, InputStream x, int length)
            throws SQLException {
        realResultSet.updateBinaryStream(columnLabel, x, length);
    }

    public void updateBinaryStream(String columnLabel, InputStream x, long length)
            throws SQLException {
        realResultSet.updateBinaryStream(columnLabel, x, length);
    }

    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        realResultSet.updateBlob(columnIndex, x);
    }

    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        realResultSet.updateBlob(columnIndex, inputStream);
    }

    public void updateBlob(int columnIndex, InputStream inputStream, long length)
            throws SQLException {
        realResultSet.updateBlob(columnIndex, inputStream, length);
    }

    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        realResultSet.updateBlob(columnLabel, x);
    }

    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        realResultSet.updateBlob(columnLabel, inputStream);
    }

    public void updateBlob(String columnLabel, InputStream inputStream, long length)
            throws SQLException {
        realResultSet.updateBlob(columnLabel, inputStream, length);
    }

    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        realResultSet.updateBoolean(columnIndex, x);
    }

    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        realResultSet.updateBoolean(columnLabel, x);
    }

    public void updateByte(int columnIndex, byte x) throws SQLException {
        realResultSet.updateByte(columnIndex, x);
    }

    public void updateByte(String columnLabel, byte x) throws SQLException {
        realResultSet.updateByte(columnLabel, x);
    }

    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        realResultSet.updateBytes(columnIndex, x);
    }

    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        realResultSet.updateBytes(columnLabel, x);
    }

    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        realResultSet.updateCharacterStream(columnIndex, x);
    }

    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        realResultSet.updateCharacterStream(columnIndex, x, length);
    }

    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        realResultSet.updateCharacterStream(columnIndex, x, length);
    }

    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        realResultSet.updateCharacterStream(columnLabel, reader);
    }

    public void updateCharacterStream(String columnLabel, Reader reader, int length)
            throws SQLException {
        realResultSet.updateCharacterStream(columnLabel, reader, length);
    }

    public void updateCharacterStream(String columnLabel, Reader reader, long length)
            throws SQLException {
        realResultSet.updateCharacterStream(columnLabel, reader, length);
    }

    public void updateClob(int columnIndex, Clob x) throws SQLException {
        realResultSet.updateClob(columnIndex, x);
    }

    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        realResultSet.updateClob(columnIndex, reader);
    }

    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        realResultSet.updateClob(columnIndex, reader, length);
    }

    public void updateClob(String columnLabel, Clob x) throws SQLException {
        realResultSet.updateClob(columnLabel, x);
    }

    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        realResultSet.updateClob(columnLabel, reader);
    }

    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        realResultSet.updateClob(columnLabel, reader, length);
    }

    public void updateDate(int columnIndex, Date x) throws SQLException {
        realResultSet.updateDate(columnIndex, x);
    }

    public void updateDate(String columnLabel, Date x) throws SQLException {
        realResultSet.updateDate(columnLabel, x);
    }

    public void updateDouble(int columnIndex, double x) throws SQLException {
        realResultSet.updateDouble(columnIndex, x);
    }

    public void updateDouble(String columnLabel, double x) throws SQLException {
        realResultSet.updateDouble(columnLabel, x);
    }

    public void updateFloat(int columnIndex, float x) throws SQLException {
        realResultSet.updateFloat(columnIndex, x);
    }

    public void updateFloat(String columnLabel, float x) throws SQLException {
        realResultSet.updateFloat(columnLabel, x);
    }

    public void updateInt(int columnIndex, int x) throws SQLException {
        realResultSet.updateInt(columnIndex, x);
    }

    public void updateInt(String columnLabel, int x) throws SQLException {
        realResultSet.updateInt(columnLabel, x);
    }

    public void updateLong(int columnIndex, long x) throws SQLException {
        realResultSet.updateLong(columnIndex, x);
    }

    public void updateLong(String columnLabel, long x) throws SQLException {
        realResultSet.updateLong(columnLabel, x);
    }

    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        realResultSet.updateNCharacterStream(columnIndex, x);
    }

    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        realResultSet.updateCharacterStream(columnIndex, x, length);
    }

    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        realResultSet.updateNCharacterStream(columnLabel, reader);
    }

    public void updateNCharacterStream(String columnLabel, Reader reader, long length)
            throws SQLException {
        realResultSet.updateNCharacterStream(columnLabel, reader, length);
    }

    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        realResultSet.updateNClob(columnIndex, nClob);
    }

    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        realResultSet.updateNClob(columnIndex, reader);
    }

    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        realResultSet.updateNClob(columnIndex, reader, length);
    }

    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        realResultSet.updateNClob(columnLabel, nClob);
    }

    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        realResultSet.updateNClob(columnLabel, reader);
    }

    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        realResultSet.updateNClob(columnLabel, reader, length);
    }

    public void updateNString(int columnIndex, String nString) throws SQLException {
        realResultSet.updateNString(columnIndex, nString);
    }

    public void updateNString(String columnLabel, String nString) throws SQLException {
        realResultSet.updateNString(columnLabel, nString);
    }

    public void updateNull(int columnIndex) throws SQLException {
        realResultSet.updateNull(columnIndex);
    }

    public void updateNull(String columnLabel) throws SQLException {
        realResultSet.updateNull(columnLabel);
    }

    public void updateObject(int columnIndex, Object x) throws SQLException {
        realResultSet.updateObject(columnIndex, x);
    }

    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        realResultSet.updateObject(columnIndex, x, scaleOrLength);
    }

    public void updateObject(String columnLabel, Object x) throws SQLException {
        realResultSet.updateObject(columnLabel, x);
    }

    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        realResultSet.updateObject(columnLabel, x, scaleOrLength);
    }

    public void updateRef(int columnIndex, Ref x) throws SQLException {
        realResultSet.updateRef(columnIndex, x);
    }

    public void updateRef(String columnLabel, Ref x) throws SQLException {
        realResultSet.updateRef(columnLabel, x);
    }

    public void updateRow() throws SQLException {
        realResultSet.updateRow();
    }

    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        realResultSet.updateRowId(columnIndex, x);
    }

    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        realResultSet.updateRowId(columnLabel, x);
    }

    public void updateShort(int columnIndex, short x) throws SQLException {
        realResultSet.updateShort(columnIndex, x);
    }

    public void updateShort(String columnLabel, short x) throws SQLException {
        realResultSet.updateShort(columnLabel, x);
    }

    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        realResultSet.updateSQLXML(columnIndex, xmlObject);
    }

    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        realResultSet.updateSQLXML(columnLabel, xmlObject);
    }

    public void updateString(int columnIndex, String x) throws SQLException {
        realResultSet.updateString(columnIndex, x);
    }

    public void updateString(String columnLabel, String x) throws SQLException {
        realResultSet.updateString(columnLabel, x);
    }

    public void updateTime(int columnIndex, Time x) throws SQLException {
        realResultSet.updateTime(columnIndex, x);
    }

    public void updateTime(String columnLabel, Time x) throws SQLException {
        realResultSet.updateTime(columnLabel, x);
    }

    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        realResultSet.updateTimestamp(columnIndex, x);
    }

    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        realResultSet.updateTimestamp(columnLabel, x);
    }

    public boolean wasNull() throws SQLException {
        return realResultSet.wasNull();
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
