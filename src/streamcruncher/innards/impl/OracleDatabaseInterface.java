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
package streamcruncher.innards.impl;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;
import java.util.Properties;

import oracle.sql.TIMESTAMP;
import streamcruncher.api.DBName;
import streamcruncher.innards.core.partition.aggregate.AbstractAggregatedColumnDDLHelper;
import streamcruncher.innards.db.DatabaseInterface;
import streamcruncher.innards.impl.query.DDLHelper;
import streamcruncher.innards.impl.query.OracleDDLHelper;
import streamcruncher.innards.impl.query.OracleParser;
import streamcruncher.innards.query.Parser;
import streamcruncher.innards.util.CallableStatementWrapper;
import streamcruncher.innards.util.ConnectionWrapper;
import streamcruncher.innards.util.CustomDriver;
import streamcruncher.innards.util.PreparedStatementWrapper;
import streamcruncher.innards.util.ResultSetWrapper;
import streamcruncher.innards.util.StatementWrapper;

/*
 * Author: Ashwin Jayaprakash Date: Jul 22, 2006 Time: 6:52:50 PM
 */

public class OracleDatabaseInterface extends DatabaseInterface {
    @Override
    public void start(Object... params) throws Exception {
        Properties props = (Properties) params[0];

        OracleDriverAdapter adapter = new OracleDriverAdapter(props);
        DriverManager.registerDriver(adapter);

        // -----------

        super.start(params);
    }

    @Override
    public Class<? extends Parser> getParser() {
        return OracleParser.class;
    }

    @Override
    public DBName getDBName() {
        return DBName.Oracle;
    }

    @Override
    public AbstractAggregatedColumnDDLHelper getAggregatedColumnDDLHelper() {
        return new OracleDDLHelper();
    }

    @Override
    public DDLHelper getDDLHelper() {
        return new OracleDDLHelper();
    }

    // ------------

    public static class OracleDriverAdapter extends CustomDriver {
        public OracleDriverAdapter(Properties properties) throws InstantiationException,
                IllegalAccessException, ClassNotFoundException {
            super(properties);
        }

        @Override
        protected Connection wrapNewConnection(Connection connection) {
            OracleConnectionWrapper wrapper = new OracleConnectionWrapper(connection);
            return wrapper;
        }
    }

    public static class OracleConnectionWrapper extends ConnectionWrapper {
        public OracleConnectionWrapper(Connection realConnection) {
            super(realConnection);
        }

        @Override
        public Statement createStatement() throws SQLException {
            Statement statement = super.createStatement();

            OracleStatementWrapper wrapper = new OracleStatementWrapper(this, statement);
            return wrapper;
        }

        @Override
        public Statement createStatement(int resultSetType, int resultSetConcurrency)
                throws SQLException {
            Statement statement = super.createStatement(resultSetType, resultSetConcurrency);

            OracleStatementWrapper wrapper = new OracleStatementWrapper(this, statement);
            return wrapper;
        }

        @Override
        public Statement createStatement(int resultSetType, int resultSetConcurrency,
                int resultSetHoldability) throws SQLException {
            Statement statement = super.createStatement(resultSetType, resultSetConcurrency,
                    resultSetHoldability);

            OracleStatementWrapper wrapper = new OracleStatementWrapper(this, statement);
            return wrapper;
        }

        @Override
        public CallableStatement prepareCall(String sql, int resultSetType,
                int resultSetConcurrency, int resultSetHoldability) throws SQLException {
            CallableStatement callableStatement = super.prepareCall(sql, resultSetType,
                    resultSetConcurrency, resultSetHoldability);

            OracleCallableStatementWrapper wrapper = new OracleCallableStatementWrapper(this,
                    callableStatement);
            return wrapper;
        }

        @Override
        public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
                throws SQLException {
            CallableStatement callableStatement = super.prepareCall(sql, resultSetType,
                    resultSetConcurrency);

            OracleCallableStatementWrapper wrapper = new OracleCallableStatementWrapper(this,
                    callableStatement);
            return wrapper;
        }

        @Override
        public CallableStatement prepareCall(String sql) throws SQLException {
            CallableStatement callableStatement = super.prepareCall(sql);

            OracleCallableStatementWrapper wrapper = new OracleCallableStatementWrapper(this,
                    callableStatement);
            return wrapper;
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int resultSetType,
                int resultSetConcurrency, int resultSetHoldability) throws SQLException {
            PreparedStatement preparedStatement = super.prepareStatement(sql, resultSetType,
                    resultSetConcurrency, resultSetHoldability);

            OraclePreparedStatementWrapper wrapper = new OraclePreparedStatementWrapper(this,
                    preparedStatement);
            return wrapper;
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int resultSetType,
                int resultSetConcurrency) throws SQLException {
            PreparedStatement preparedStatement = super.prepareStatement(sql, resultSetType,
                    resultSetConcurrency);

            OraclePreparedStatementWrapper wrapper = new OraclePreparedStatementWrapper(this,
                    preparedStatement);
            return wrapper;
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
                throws SQLException {
            PreparedStatement preparedStatement = super.prepareStatement(sql, autoGeneratedKeys);

            OraclePreparedStatementWrapper wrapper = new OraclePreparedStatementWrapper(this,
                    preparedStatement);
            return wrapper;
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
                throws SQLException {
            PreparedStatement preparedStatement = super.prepareStatement(sql, columnIndexes);

            OraclePreparedStatementWrapper wrapper = new OraclePreparedStatementWrapper(this,
                    preparedStatement);
            return wrapper;
        }

        @Override
        public PreparedStatement prepareStatement(String sql, String[] columnNames)
                throws SQLException {
            PreparedStatement preparedStatement = super.prepareStatement(sql, columnNames);

            OraclePreparedStatementWrapper wrapper = new OraclePreparedStatementWrapper(this,
                    preparedStatement);
            return wrapper;
        }

        @Override
        public PreparedStatement prepareStatement(String sql) throws SQLException {
            PreparedStatement preparedStatement = super.prepareStatement(sql);

            OraclePreparedStatementWrapper wrapper = new OraclePreparedStatementWrapper(this,
                    preparedStatement);
            return wrapper;
        }
    }

    public static class OracleStatementWrapper extends StatementWrapper {
        public OracleStatementWrapper(Connection wrappedConnection, Statement realStatement) {
            super(wrappedConnection, realStatement);
        }

        @Override
        public ResultSet executeQuery(String sql) throws SQLException {
            ResultSet resultSet = super.executeQuery(sql);

            OracleResultSetWrapper wrapper = new OracleResultSetWrapper(this, resultSet);
            return wrapper;
        }

        @Override
        public ResultSet getResultSet() throws SQLException {
            ResultSet resultSet = super.getResultSet();

            OracleResultSetWrapper wrapper = new OracleResultSetWrapper(this, resultSet);
            return wrapper;
        }
    }

    public static class OraclePreparedStatementWrapper extends PreparedStatementWrapper {
        public OraclePreparedStatementWrapper(Connection wrappedConnection,
                PreparedStatement realStatement) {
            super(wrappedConnection, realStatement);
        }

        @Override
        public ResultSet executeQuery() throws SQLException {
            ResultSet resultSet = super.executeQuery();

            OracleResultSetWrapper wrapper = new OracleResultSetWrapper(this, resultSet);
            return wrapper;
        }

        @Override
        public ResultSet executeQuery(String sql) throws SQLException {
            ResultSet resultSet = super.executeQuery(sql);

            OracleResultSetWrapper wrapper = new OracleResultSetWrapper(this, resultSet);
            return wrapper;
        }

        @Override
        public ResultSet getResultSet() throws SQLException {
            ResultSet resultSet = super.getResultSet();

            OracleResultSetWrapper wrapper = new OracleResultSetWrapper(this, resultSet);
            return wrapper;
        }
    }

    public static class OracleCallableStatementWrapper extends CallableStatementWrapper {
        public OracleCallableStatementWrapper(Connection wrappedConnection,
                CallableStatement realStatement) {
            super(wrappedConnection, realStatement);
        }

        @Override
        public ResultSet executeQuery() throws SQLException {
            ResultSet resultSet = super.executeQuery();

            OracleResultSetWrapper wrapper = new OracleResultSetWrapper(this, resultSet);
            return wrapper;
        }

        @Override
        public ResultSet executeQuery(String sql) throws SQLException {
            ResultSet resultSet = super.executeQuery(sql);

            OracleResultSetWrapper wrapper = new OracleResultSetWrapper(this, resultSet);
            return wrapper;
        }

        @Override
        public ResultSet getResultSet() throws SQLException {
            ResultSet resultSet = super.getResultSet();

            OracleResultSetWrapper wrapper = new OracleResultSetWrapper(this, resultSet);
            return wrapper;
        }
    }

    /**
     * Oracle Driver has a problem handling ResultSet.getTimestamp(..)
     * correctly. Such calls are intercepted and handled by the Kernel.
     */
    public static class OracleResultSetWrapper extends ResultSetWrapper {
        public OracleResultSetWrapper(Statement wrappedStatement, ResultSet realResultSet)
                throws SQLException {
            super(wrappedStatement, realResultSet);
        }

        protected Object checkAndConvertToTS(Object obj) throws SQLException {
            Object retVal = obj;

            // todo Remove this compile-time dependency on Oracle Driver.
            if (obj != null && obj instanceof TIMESTAMP) {
                TIMESTAMP ts = (TIMESTAMP) obj;
                Timestamp timestamp = ts.timestampValue();
                retVal = timestamp;
            }

            return retVal;
        }

        @Override
        public Object getObject(int arg0, Map<String, Class<?>> arg1) throws SQLException {
            Object obj = super.getObject(arg0, arg1);

            obj = checkAndConvertToTS(obj);

            return obj;
        }

        @Override
        public Object getObject(int parameterIndex) throws SQLException {
            Object obj = super.getObject(parameterIndex);

            obj = checkAndConvertToTS(obj);

            return obj;
        }

        @Override
        public Object getObject(String arg0, Map<String, Class<?>> arg1) throws SQLException {
            Object obj = super.getObject(arg0, arg1);

            obj = checkAndConvertToTS(obj);

            return obj;
        }

        @Override
        public Object getObject(String parameterName) throws SQLException {
            Object obj = super.getObject(parameterName);

            obj = checkAndConvertToTS(obj);

            return obj;
        }

        @Override
        public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
            Object obj = super.getTimestamp(parameterIndex, cal);

            Timestamp ts = (Timestamp) checkAndConvertToTS(obj);

            return ts;
        }

        @Override
        public Timestamp getTimestamp(int parameterIndex) throws SQLException {
            Object obj = super.getTimestamp(parameterIndex);

            Timestamp ts = (Timestamp) checkAndConvertToTS(obj);

            return ts;
        }

        @Override
        public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
            Object obj = super.getTimestamp(parameterName, cal);

            Timestamp ts = (Timestamp) checkAndConvertToTS(obj);

            return ts;
        }

        @Override
        public Timestamp getTimestamp(String parameterName) throws SQLException {
            Object obj = super.getTimestamp(parameterName);

            Timestamp ts = (Timestamp) checkAndConvertToTS(obj);

            return ts;
        }
    }
}