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
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

/*
 * Author: Ashwin Jayaprakash Date: Jan 6, 2007 Time: 9:46:43 AM
 */

/**
 * Interface methods as of JDK 1.6.
 */
public abstract class CallableStatementWrapper extends PreparedStatementWrapper implements
        CallableStatement {
    protected final CallableStatement realCallableStatement;

    protected CallableStatementWrapper(Connection wrappedConnection, CallableStatement realStatement) {
        super(wrappedConnection, realStatement);

        this.realCallableStatement = realStatement;
    }

    public Array getArray(int parameterIndex) throws SQLException {
        return realCallableStatement.getArray(parameterIndex);
    }

    public Array getArray(String parameterName) throws SQLException {
        return realCallableStatement.getArray(parameterName);
    }

    /**
     * {@inheritDoc}
     * 
     * @deprecated
     */
    public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
        return realCallableStatement.getBigDecimal(parameterIndex, scale);
    }

    public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
        return realCallableStatement.getBigDecimal(parameterIndex);
    }

    public BigDecimal getBigDecimal(String parameterName) throws SQLException {
        return realCallableStatement.getBigDecimal(parameterName);
    }

    public Blob getBlob(int parameterIndex) throws SQLException {
        return realCallableStatement.getBlob(parameterIndex);
    }

    public Blob getBlob(String parameterName) throws SQLException {
        return realCallableStatement.getBlob(parameterName);
    }

    public boolean getBoolean(int parameterIndex) throws SQLException {
        return realCallableStatement.getBoolean(parameterIndex);
    }

    public boolean getBoolean(String parameterName) throws SQLException {
        return realCallableStatement.getBoolean(parameterName);
    }

    public byte getByte(int parameterIndex) throws SQLException {
        return realCallableStatement.getByte(parameterIndex);
    }

    public byte getByte(String parameterName) throws SQLException {
        return realCallableStatement.getByte(parameterName);
    }

    public byte[] getBytes(int parameterIndex) throws SQLException {
        return realCallableStatement.getBytes(parameterIndex);
    }

    public byte[] getBytes(String parameterName) throws SQLException {
        return realCallableStatement.getBytes(parameterName);
    }

    public Reader getCharacterStream(int parameterIndex) throws SQLException {
        return realCallableStatement.getCharacterStream(parameterIndex);
    }

    public Reader getCharacterStream(String parameterName) throws SQLException {
        return realCallableStatement.getCharacterStream(parameterName);
    }

    public Clob getClob(int parameterIndex) throws SQLException {
        return realCallableStatement.getClob(parameterIndex);
    }

    public Clob getClob(String parameterName) throws SQLException {
        return realCallableStatement.getClob(parameterName);
    }

    public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
        return realCallableStatement.getDate(parameterIndex, cal);
    }

    public Date getDate(int parameterIndex) throws SQLException {
        return realCallableStatement.getDate(parameterIndex);
    }

    public Date getDate(String parameterName, Calendar cal) throws SQLException {
        return realCallableStatement.getDate(parameterName, cal);
    }

    public Date getDate(String parameterName) throws SQLException {
        return realCallableStatement.getDate(parameterName);
    }

    public double getDouble(int parameterIndex) throws SQLException {
        return realCallableStatement.getDouble(parameterIndex);
    }

    public double getDouble(String parameterName) throws SQLException {
        return realCallableStatement.getDouble(parameterName);
    }

    public float getFloat(int parameterIndex) throws SQLException {
        return realCallableStatement.getFloat(parameterIndex);
    }

    public float getFloat(String parameterName) throws SQLException {
        return realCallableStatement.getFloat(parameterName);
    }

    public int getInt(int parameterIndex) throws SQLException {
        return realCallableStatement.getInt(parameterIndex);
    }

    public int getInt(String parameterName) throws SQLException {
        return realCallableStatement.getInt(parameterName);
    }

    public long getLong(int parameterIndex) throws SQLException {
        return realCallableStatement.getLong(parameterIndex);
    }

    public long getLong(String parameterName) throws SQLException {
        return realCallableStatement.getLong(parameterName);
    }

    public Reader getNCharacterStream(int parameterIndex) throws SQLException {
        return realCallableStatement.getNCharacterStream(parameterIndex);
    }

    public Reader getNCharacterStream(String parameterName) throws SQLException {
        return realCallableStatement.getNCharacterStream(parameterName);
    }

    public NClob getNClob(int parameterIndex) throws SQLException {
        return realCallableStatement.getNClob(parameterIndex);
    }

    public NClob getNClob(String parameterName) throws SQLException {
        return realCallableStatement.getNClob(parameterName);
    }

    public String getNString(int parameterIndex) throws SQLException {
        return realCallableStatement.getNString(parameterIndex);
    }

    public String getNString(String parameterName) throws SQLException {
        return realCallableStatement.getNString(parameterName);
    }

    public Object getObject(int arg0, Map<String, Class<?>> arg1) throws SQLException {
        return realCallableStatement.getObject(arg0, arg1);
    }

    public Object getObject(int parameterIndex) throws SQLException {
        return realCallableStatement.getObject(parameterIndex);
    }

    public Object getObject(String arg0, Map<String, Class<?>> arg1) throws SQLException {
        return realCallableStatement.getObject(arg0, arg1);
    }

    public Object getObject(String parameterName) throws SQLException {
        return realCallableStatement.getObject(parameterName);
    }

    public Ref getRef(int parameterIndex) throws SQLException {
        return realCallableStatement.getRef(parameterIndex);
    }

    public Ref getRef(String parameterName) throws SQLException {
        return realCallableStatement.getRef(parameterName);
    }

    public RowId getRowId(int parameterIndex) throws SQLException {
        return realCallableStatement.getRowId(parameterIndex);
    }

    public RowId getRowId(String parameterName) throws SQLException {
        return realCallableStatement.getRowId(parameterName);
    }

    public short getShort(int parameterIndex) throws SQLException {
        return realCallableStatement.getShort(parameterIndex);
    }

    public short getShort(String parameterName) throws SQLException {
        return realCallableStatement.getShort(parameterName);
    }

    public SQLXML getSQLXML(int parameterIndex) throws SQLException {
        return realCallableStatement.getSQLXML(parameterIndex);
    }

    public SQLXML getSQLXML(String parameterName) throws SQLException {
        return realCallableStatement.getSQLXML(parameterName);
    }

    public String getString(int parameterIndex) throws SQLException {
        return realCallableStatement.getString(parameterIndex);
    }

    public String getString(String parameterName) throws SQLException {
        return realCallableStatement.getString(parameterName);
    }

    public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
        return realCallableStatement.getTime(parameterIndex, cal);
    }

    public Time getTime(int parameterIndex) throws SQLException {
        return realCallableStatement.getTime(parameterIndex);
    }

    public Time getTime(String parameterName, Calendar cal) throws SQLException {
        return realCallableStatement.getTime(parameterName, cal);
    }

    public Time getTime(String parameterName) throws SQLException {
        return realCallableStatement.getTime(parameterName);
    }

    public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
        return realCallableStatement.getTimestamp(parameterIndex, cal);
    }

    public Timestamp getTimestamp(int parameterIndex) throws SQLException {
        return realCallableStatement.getTimestamp(parameterIndex);
    }

    public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
        return realCallableStatement.getTimestamp(parameterName, cal);
    }

    public Timestamp getTimestamp(String parameterName) throws SQLException {
        return realCallableStatement.getTimestamp(parameterName);
    }

    public URL getURL(int parameterIndex) throws SQLException {
        return realCallableStatement.getURL(parameterIndex);
    }

    public URL getURL(String parameterName) throws SQLException {
        return realCallableStatement.getURL(parameterName);
    }

    public void registerOutParameter(int parameterIndex, int sqlType, int scale)
            throws SQLException {
        realCallableStatement.registerOutParameter(parameterIndex, sqlType, scale);
    }

    public void registerOutParameter(int parameterIndex, int sqlType, String typeName)
            throws SQLException {
        realCallableStatement.registerOutParameter(parameterIndex, sqlType, typeName);
    }

    public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
        realCallableStatement.registerOutParameter(parameterIndex, sqlType);
    }

    public void registerOutParameter(String parameterName, int sqlType, int scale)
            throws SQLException {
        realCallableStatement.registerOutParameter(parameterName, sqlType, scale);
    }

    public void registerOutParameter(String parameterName, int sqlType, String typeName)
            throws SQLException {
        realCallableStatement.registerOutParameter(parameterName, sqlType, typeName);
    }

    public void registerOutParameter(String parameterName, int sqlType) throws SQLException {
        realCallableStatement.registerOutParameter(parameterName, sqlType);
    }

    public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {
        realCallableStatement.setAsciiStream(parameterName, x, length);
    }

    public void setAsciiStream(String parameterName, InputStream x, long length)
            throws SQLException {
        realCallableStatement.setAsciiStream(parameterName, x, length);
    }

    public void setAsciiStream(String parameterName, InputStream x) throws SQLException {
        realCallableStatement.setAsciiStream(parameterName, x);
    }

    public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
        realCallableStatement.setBigDecimal(parameterName, x);
    }

    public void setBinaryStream(String parameterName, InputStream x, int length)
            throws SQLException {
        realCallableStatement.setBinaryStream(parameterName, x, length);
    }

    public void setBinaryStream(String parameterName, InputStream x, long length)
            throws SQLException {
        realCallableStatement.setBinaryStream(parameterName, x, length);
    }

    public void setBinaryStream(String parameterName, InputStream x) throws SQLException {
        realCallableStatement.setBinaryStream(parameterName, x);
    }

    public void setBlob(String parameterName, Blob x) throws SQLException {
        realCallableStatement.setBlob(parameterName, x);
    }

    public void setBlob(String parameterName, InputStream inputStream, long length)
            throws SQLException {
        realCallableStatement.setBlob(parameterName, inputStream, length);
    }

    public void setBlob(String parameterName, InputStream inputStream) throws SQLException {
        realCallableStatement.setBlob(parameterName, inputStream);
    }

    public void setBoolean(String parameterName, boolean x) throws SQLException {
        realCallableStatement.setBoolean(parameterName, x);
    }

    public void setByte(String parameterName, byte x) throws SQLException {
        realCallableStatement.setByte(parameterName, x);
    }

    public void setBytes(String parameterName, byte[] x) throws SQLException {
        realCallableStatement.setBytes(parameterName, x);
    }

    public void setCharacterStream(String parameterName, Reader reader, int length)
            throws SQLException {
        realCallableStatement.setCharacterStream(parameterName, reader, length);
    }

    public void setCharacterStream(String parameterName, Reader reader, long length)
            throws SQLException {
        realCallableStatement.setCharacterStream(parameterName, reader, length);
    }

    public void setCharacterStream(String parameterName, Reader reader) throws SQLException {
        realCallableStatement.setCharacterStream(parameterName, reader);
    }

    public void setClob(String parameterName, Clob x) throws SQLException {
        realCallableStatement.setClob(parameterName, x);
    }

    public void setClob(String parameterName, Reader reader, long length) throws SQLException {
        realCallableStatement.setClob(parameterName, reader, length);
    }

    public void setClob(String parameterName, Reader reader) throws SQLException {
        realCallableStatement.setClob(parameterName, reader);
    }

    public void setDate(String parameterName, Date x, Calendar cal) throws SQLException {
        realCallableStatement.setDate(parameterName, x, cal);
    }

    public void setDate(String parameterName, Date x) throws SQLException {
        realCallableStatement.setDate(parameterName, x);
    }

    public void setDouble(String parameterName, double x) throws SQLException {
        realCallableStatement.setDouble(parameterName, x);
    }

    public void setFloat(String parameterName, float x) throws SQLException {
        realCallableStatement.setFloat(parameterName, x);
    }

    public void setInt(String parameterName, int x) throws SQLException {
        realCallableStatement.setInt(parameterName, x);
    }

    public void setLong(String parameterName, long x) throws SQLException {
        realCallableStatement.setLong(parameterName, x);
    }

    public void setNCharacterStream(String parameterName, Reader value, long length)
            throws SQLException {
        realCallableStatement.setNCharacterStream(parameterName, value, length);
    }

    public void setNCharacterStream(String parameterName, Reader value) throws SQLException {
        realCallableStatement.setNCharacterStream(parameterName, value);
    }

    public void setNClob(String parameterName, NClob value) throws SQLException {
        realCallableStatement.setNClob(parameterName, value);
    }

    public void setNClob(String parameterName, Reader reader, long length) throws SQLException {
        realCallableStatement.setNCharacterStream(parameterName, reader, length);
    }

    public void setNClob(String parameterName, Reader reader) throws SQLException {
        realCallableStatement.setNCharacterStream(parameterName, reader);
    }

    public void setNString(String parameterName, String value) throws SQLException {
        realCallableStatement.setNString(parameterName, value);
    }

    public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
        realCallableStatement.setNull(parameterName, sqlType, typeName);
    }

    public void setNull(String parameterName, int sqlType) throws SQLException {
        realCallableStatement.setNull(parameterName, sqlType);
    }

    public void setObject(String parameterName, Object x, int targetSqlType, int scale)
            throws SQLException {
        realCallableStatement.setObject(parameterName, x, targetSqlType, scale);
    }

    public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {
        realCallableStatement.setObject(parameterName, x, targetSqlType);
    }

    public void setObject(String parameterName, Object x) throws SQLException {
        realCallableStatement.setObject(parameterName, x);
    }

    public void setRowId(String parameterName, RowId x) throws SQLException {
        realCallableStatement.setRowId(parameterName, x);
    }

    public void setShort(String parameterName, short x) throws SQLException {
        realCallableStatement.setShort(parameterName, x);
    }

    public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
        realCallableStatement.setSQLXML(parameterName, xmlObject);
    }

    public void setString(String parameterName, String x) throws SQLException {
        realCallableStatement.setString(parameterName, x);
    }

    public void setTime(String parameterName, Time x, Calendar cal) throws SQLException {
        realCallableStatement.setTime(parameterName, x, cal);
    }

    public void setTime(String parameterName, Time x) throws SQLException {
        realCallableStatement.setTime(parameterName, x);
    }

    public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {
        realCallableStatement.setTimestamp(parameterName, x, cal);
    }

    public void setTimestamp(String parameterName, Timestamp x) throws SQLException {
        realCallableStatement.setTimestamp(parameterName, x);
    }

    public void setURL(String parameterName, URL val) throws SQLException {
        realCallableStatement.setURL(parameterName, val);
    }

    public boolean wasNull() throws SQLException {
        return realCallableStatement.wasNull();
    }
}
