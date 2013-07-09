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
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

/*
 * Author: Ashwin Jayaprakash Date: Jan 6, 2007 Time: 9:37:12 AM
 */

/**
 * Interface methods as of JDK 1.6.
 */
public abstract class PreparedStatementWrapper extends StatementWrapper implements
        java.sql.PreparedStatement {
    protected final PreparedStatement realPreparedStatement;

    protected PreparedStatementWrapper(Connection wrappedConnection, PreparedStatement realStatement) {
        super(wrappedConnection, realStatement);

        this.realPreparedStatement = realStatement;
    }

    public void addBatch() throws SQLException {
        this.realPreparedStatement.addBatch();
    }

    public void clearParameters() throws SQLException {
        this.realPreparedStatement.clearParameters();
    }

    public boolean execute() throws SQLException {
        return this.realPreparedStatement.execute();
    }

    public ResultSet executeQuery() throws SQLException {
        return this.realPreparedStatement.executeQuery();
    }

    public int executeUpdate() throws SQLException {
        return this.realPreparedStatement.executeUpdate();
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        return this.realPreparedStatement.getMetaData();
    }

    public ParameterMetaData getParameterMetaData() throws SQLException {
        return this.realPreparedStatement.getParameterMetaData();
    }

    public void setArray(int parameterIndex, Array x) throws SQLException {
        this.realPreparedStatement.setArray(parameterIndex, x);
    }

    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        this.realPreparedStatement.setAsciiStream(parameterIndex, x);
    }

    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        this.realPreparedStatement.setAsciiStream(parameterIndex, x, length);
    }

    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        this.realPreparedStatement.setAsciiStream(parameterIndex, x, length);
    }

    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        this.realPreparedStatement.setBigDecimal(parameterIndex, x);
    }

    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        setBinaryStream(parameterIndex, x);
    }

    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        this.realPreparedStatement.setBinaryStream(parameterIndex, x, length);
    }

    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        this.realPreparedStatement.setBinaryStream(parameterIndex, x, length);
    }

    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        this.realPreparedStatement.setBlob(parameterIndex, x);
    }

    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        this.realPreparedStatement.setBlob(parameterIndex, inputStream);
    }

    public void setBlob(int parameterIndex, InputStream inputStream, long length)
            throws SQLException {
        this.realPreparedStatement.setBlob(parameterIndex, inputStream, length);
    }

    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        this.realPreparedStatement.setBoolean(parameterIndex, x);
    }

    public void setByte(int parameterIndex, byte x) throws SQLException {
        this.realPreparedStatement.setByte(parameterIndex, x);
    }

    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        this.realPreparedStatement.setBytes(parameterIndex, x);
    }

    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        this.realPreparedStatement.setCharacterStream(parameterIndex, reader);
    }

    public void setCharacterStream(int parameterIndex, Reader reader, int length)
            throws SQLException {
        this.realPreparedStatement.setCharacterStream(parameterIndex, reader, length);
    }

    public void setCharacterStream(int parameterIndex, Reader reader, long length)
            throws SQLException {
        this.realPreparedStatement.setCharacterStream(parameterIndex, reader, length);
    }

    public void setClob(int parameterIndex, Clob x) throws SQLException {
        this.realPreparedStatement.setClob(parameterIndex, x);
    }

    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        this.realPreparedStatement.setClob(parameterIndex, reader);
    }

    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        this.realPreparedStatement.setClob(parameterIndex, reader, length);
    }

    public void setDate(int parameterIndex, Date x) throws SQLException {
        this.realPreparedStatement.setDate(parameterIndex, x);
    }

    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        this.realPreparedStatement.setDate(parameterIndex, x, cal);
    }

    public void setDouble(int parameterIndex, double x) throws SQLException {
        this.realPreparedStatement.setDouble(parameterIndex, x);
    }

    public void setFloat(int parameterIndex, float x) throws SQLException {
        this.realPreparedStatement.setFloat(parameterIndex, x);
    }

    public void setInt(int parameterIndex, int x) throws SQLException {
        this.realPreparedStatement.setInt(parameterIndex, x);
    }

    public void setLong(int parameterIndex, long x) throws SQLException {
        this.realPreparedStatement.setLong(parameterIndex, x);
    }

    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        this.realPreparedStatement.setNCharacterStream(parameterIndex, value);
    }

    public void setNCharacterStream(int parameterIndex, Reader value, long length)
            throws SQLException {
        this.realPreparedStatement.setNCharacterStream(parameterIndex, value, length);
    }

    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        this.realPreparedStatement.setNClob(parameterIndex, value);
    }

    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        this.realPreparedStatement.setNClob(parameterIndex, reader);
    }

    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        this.realPreparedStatement.setNClob(parameterIndex, reader, length);
    }

    public void setNString(int parameterIndex, String value) throws SQLException {
        this.realPreparedStatement.setNString(parameterIndex, value);
    }

    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        this.realPreparedStatement.setNull(parameterIndex, sqlType);
    }

    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        this.realPreparedStatement.setNull(parameterIndex, sqlType, typeName);
    }

    public void setObject(int parameterIndex, Object x) throws SQLException {
        this.realPreparedStatement.setObject(parameterIndex, x);
    }

    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        this.realPreparedStatement.setObject(parameterIndex, x, targetSqlType);
    }

    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)
            throws SQLException {
        this.realPreparedStatement.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
    }

    public void setRef(int parameterIndex, Ref x) throws SQLException {
        this.realPreparedStatement.setRef(parameterIndex, x);
    }

    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        this.realPreparedStatement.setRowId(parameterIndex, x);
    }

    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        this.realPreparedStatement.setSQLXML(parameterIndex, xmlObject);
    }

    public void setShort(int parameterIndex, short x) throws SQLException {
        this.realPreparedStatement.setShort(parameterIndex, x);
    }

    public void setString(int parameterIndex, String x) throws SQLException {
        this.realPreparedStatement.setString(parameterIndex, x);
    }

    public void setTime(int parameterIndex, Time x) throws SQLException {
        this.realPreparedStatement.setTime(parameterIndex, x);
    }

    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        this.realPreparedStatement.setTime(parameterIndex, x, cal);
    }

    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        this.realPreparedStatement.setTimestamp(parameterIndex, x);
    }

    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        this.realPreparedStatement.setTimestamp(parameterIndex, x, cal);
    }

    public void setURL(int parameterIndex, URL x) throws SQLException {
        this.realPreparedStatement.setURL(parameterIndex, x);
    }

    /**
     * {@inheritDoc}
     * 
     * @deprecated
     */
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        this.realPreparedStatement.setUnicodeStream(parameterIndex, x, length);
    }
}
