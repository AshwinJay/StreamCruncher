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
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import streamcruncher.api.DBName;
import streamcruncher.api.artifact.IndexSpec;
import streamcruncher.boot.Registry;
import streamcruncher.innards.core.partition.aggregate.AbstractAggregatedColumnDDLHelper;
import streamcruncher.innards.db.DatabaseInterface;
import streamcruncher.innards.impl.artifact.AntsIndexSpec;
import streamcruncher.innards.impl.query.AntsDDLHelper;
import streamcruncher.innards.impl.query.AntsParser;
import streamcruncher.innards.impl.query.DDLHelper;
import streamcruncher.innards.query.Parser;
import streamcruncher.innards.util.CallableStatementWrapper;
import streamcruncher.innards.util.ConnectionWrapper;
import streamcruncher.innards.util.CustomDriver;
import streamcruncher.innards.util.PreparedStatementWrapper;
import streamcruncher.util.AtomicX;
import streamcruncher.util.LoggerManager;

/*
 * Author: Ashwin Jayaprakash Date: Jul 22, 2006 Time: 7:01:25 PM
 */

public class AntsDatabaseInterface extends DatabaseInterface {
    @Override
    public void start(Object... params) throws Exception {
        Properties props = (Properties) params[0];

        AntsDriverAdapter adapter = new AntsDriverAdapter(props);
        DriverManager.registerDriver(adapter);

        // -----------

        super.start(params);
    }

    @Override
    public Class<? extends Parser> getParser() {
        return AntsParser.class;
    }

    @Override
    public DBName getDBName() {
        return DBName.Ants;
    }

    @Override
    public AbstractAggregatedColumnDDLHelper getAggregatedColumnDDLHelper() {
        return new AntsDDLHelper();
    }

    @Override
    public DDLHelper getDDLHelper() {
        return new AntsDDLHelper();
    }

    @Override
    public IndexSpec createIndexSpec(String schema, String name, String tableName, boolean unique,
            String columnName, boolean ascending) {
        return new AntsIndexSpec(schema, name, tableName, unique, columnName, ascending);
    }

    @Override
    public IndexSpec createIndexSpec(String schema, String name, String tableName, boolean unique,
            String[] columnNames, boolean[] ascending) {
        return new AntsIndexSpec(schema, name, tableName, unique, columnNames, ascending);
    }

    /**
     * <code>bigint</code> in ANTs is actually a Float. So, we use something
     * smaller than Long. And, {@link Integer#MIN_VALUE} does not work either.
     * 
     * @return
     */
    @Override
    public AtomicX createRowIdGenerator() {
        /*
         * todo AtomicInteger might not be needed - Change to AtomicLong and the
         * other Number-Long changes in Function and AbstractTablePartitioner
         * might then become unnecessary.
         */
        return new AtomicX(new AtomicInteger(0));
    }

    // -----------

    /**
     * <p>
     * doc The most bizarre thing about ANTs is that the Driver they have
     * recommended (as of 3.6 GA), does not even get registered with the
     * DriverManager. And so, every thing goes through the regular Sun JDBC-ODBC
     * Bridge. And the Sun Driver is awful!!! Loads of bugs in it.
     * </p>
     * <p>
     * <b>Bugs in Sun Driver with ANTs:</b>
     * </p>
     * <p>
     * ResultSet bug - If "getString(..)" methods are accessed repeatedly for a
     * Row or accessed in an order different from the Columns in the Select
     * clause. The error thrown is "No data found".
     * </p>
     * <p>
     * Batch Update does not work with ANTs. The Id columns gets assigned some
     * random numbers and completely ignore the numbers set in the
     * PreparedStatementWrapper.
     * </p>
     */
    public static class AntsDriverAdapter extends CustomDriver {
        public AntsDriverAdapter(Properties properties) throws InstantiationException,
                IllegalAccessException, ClassNotFoundException {
            super(properties);
        }

        @Override
        protected Connection wrapNewConnection(Connection connection) {
            AntsConnectionWrapper wrapper = new AntsConnectionWrapper(connection);
            return wrapper;
        }
    }

    public static class AntsConnectionWrapper extends ConnectionWrapper {
        public AntsConnectionWrapper(Connection realConnection) {
            super(realConnection);
        }

        @Override
        public CallableStatement prepareCall(String sql, int resultSetType,
                int resultSetConcurrency, int resultSetHoldability) throws SQLException {
            CallableStatement callableStatement = super.prepareCall(sql, resultSetType,
                    resultSetConcurrency, resultSetHoldability);

            AntsCallableStatementWrapper wrapper = new AntsCallableStatementWrapper(this,
                    callableStatement);
            return wrapper;
        }

        @Override
        public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
                throws SQLException {
            CallableStatement callableStatement = super.prepareCall(sql, resultSetType,
                    resultSetConcurrency);

            AntsCallableStatementWrapper wrapper = new AntsCallableStatementWrapper(this,
                    callableStatement);
            return wrapper;
        }

        @Override
        public CallableStatement prepareCall(String sql) throws SQLException {
            CallableStatement callableStatement = super.prepareCall(sql);

            AntsCallableStatementWrapper wrapper = new AntsCallableStatementWrapper(this,
                    callableStatement);
            return wrapper;
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int resultSetType,
                int resultSetConcurrency, int resultSetHoldability) throws SQLException {
            PreparedStatement preparedStatement = super.prepareStatement(sql, resultSetType,
                    resultSetConcurrency, resultSetHoldability);

            AntsPreparedStatementWrapper wrapper = new AntsPreparedStatementWrapper(this,
                    preparedStatement);
            return wrapper;
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int resultSetType,
                int resultSetConcurrency) throws SQLException {
            PreparedStatement preparedStatement = super.prepareStatement(sql, resultSetType,
                    resultSetConcurrency);

            AntsPreparedStatementWrapper wrapper = new AntsPreparedStatementWrapper(this,
                    preparedStatement);
            return wrapper;
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
                throws SQLException {
            PreparedStatement preparedStatement = super.prepareStatement(sql, autoGeneratedKeys);

            AntsPreparedStatementWrapper wrapper = new AntsPreparedStatementWrapper(this,
                    preparedStatement);
            return wrapper;
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
                throws SQLException {
            PreparedStatement preparedStatement = super.prepareStatement(sql, columnIndexes);

            AntsPreparedStatementWrapper wrapper = new AntsPreparedStatementWrapper(this,
                    preparedStatement);
            return wrapper;
        }

        @Override
        public PreparedStatement prepareStatement(String sql, String[] columnNames)
                throws SQLException {
            PreparedStatement preparedStatement = super.prepareStatement(sql, columnNames);

            AntsPreparedStatementWrapper wrapper = new AntsPreparedStatementWrapper(this,
                    preparedStatement);
            return wrapper;
        }

        @Override
        public PreparedStatement prepareStatement(String sql) throws SQLException {
            PreparedStatement preparedStatement = super.prepareStatement(sql);

            AntsPreparedStatementWrapper wrapper = new AntsPreparedStatementWrapper(this,
                    preparedStatement);
            return wrapper;
        }
    }

    private static final String ANTS_LOG_MSG = "ANTs Driver (3.6 GA) has a problem handling"
            + " Longs and -ve Integers less than or equal to " + Integer.MIN_VALUE
            + ", in PreparedStatements/CallableStatements."
            + " You can safely use only the setInt(..) method instead"
            + " and use numbers greater than or equal to " + (Integer.MIN_VALUE + 1);

    public static class AntsPreparedStatementWrapper extends PreparedStatementWrapper {
        protected static final String log_msg = ANTS_LOG_MSG;

        protected final ParameterMetaData metaData;

        protected final Logger logger;

        public AntsPreparedStatementWrapper(Connection wrappedConnection,
                PreparedStatement realStatement) throws SQLException {
            super(wrappedConnection, realStatement);

            this.metaData = realStatement.getParameterMetaData();

            this.logger = Registry.getImplFor(LoggerManager.class).getLogger(
                    AntsDatabaseInterface.class.getName());
        }

        @Override
        public void setLong(int parameterIndex, long x) throws SQLException {
            int i = (int) x;

            /*
             * Replace the Long value with its Integer part. The Kernel uses
             * Long everywhere for the Ids. But ANTs needs "Float" for the
             * "bigint" type. Instead of re-coding the Kernel, this is being
             * done.
             */
            if (i == x && i > Integer.MIN_VALUE) {
                super.setInt(parameterIndex, i);
            }
            /*
             * If the long value was intentionally sent then, fall thru and let
             * the DB handle it.
             */
            else {
                logger.log(Level.WARNING, log_msg);

                super.setLong(parameterIndex, x);
            }
        }

        @Override
        public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)
                throws SQLException {
            if (targetSqlType == Types.BIGINT && x != null && x instanceof Long) {
                setLong(parameterIndex, (Long) x);
            }
            else {
                super.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
            }
        }

        @Override
        public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
            if (targetSqlType == Types.BIGINT && x != null && x instanceof Long) {
                setLong(parameterIndex, (Long) x);
            }
            else {
                super.setObject(parameterIndex, x, targetSqlType);
            }
        }

        @Override
        public void setObject(int parameterIndex, Object x) throws SQLException {
            if (metaData.getParameterType(parameterIndex) == Types.BIGINT && x != null
                    && x instanceof Long) {
                setLong(parameterIndex, (Long) x);
            }
            else {
                super.setObject(parameterIndex, x);
            }
        }
    }

    public static class AntsCallableStatementWrapper extends CallableStatementWrapper {
        protected static final String log_msg = ANTS_LOG_MSG;

        protected final ParameterMetaData metaData;

        protected final Logger logger;

        public AntsCallableStatementWrapper(Connection wrappedConnection,
                CallableStatement realStatement) throws SQLException {
            super(wrappedConnection, realStatement);

            this.metaData = realStatement.getParameterMetaData();

            this.logger = Registry.getImplFor(LoggerManager.class).getLogger(
                    AntsDatabaseInterface.class.getName());
        }

        @Override
        public void setLong(int parameterIndex, long x) throws SQLException {
            int i = (int) x;

            /*
             * Replace the Long value with its Integer part. The Kernel uses
             * Long everywhere for the Ids. But ANTs needs "Float" for the
             * "bigint" type. Instead of re-coding the Kernel, this is being
             * done.
             */
            if (i == x && i > Integer.MIN_VALUE) {
                super.setInt(parameterIndex, i);
            }
            /*
             * If the long value was intentionally sent then, fall thru and let
             * the DB handle it.
             */
            else {
                logger.log(Level.WARNING, log_msg);

                super.setLong(parameterIndex, x);
            }
        }

        @Override
        public void setLong(String parameterName, long x) throws SQLException {
            int i = (int) x;

            if (i == x && i > Integer.MIN_VALUE) {
                super.setInt(parameterName, i);
            }
            else {
                logger.log(Level.WARNING, log_msg);

                super.setLong(parameterName, x);
            }
        }

        @Override
        public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)
                throws SQLException {
            if (targetSqlType == Types.BIGINT && x != null && x instanceof Long) {
                setLong(parameterIndex, (Long) x);
            }
            else {
                super.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
            }
        }

        @Override
        public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
            if (targetSqlType == Types.BIGINT && x != null && x instanceof Long) {
                setLong(parameterIndex, (Long) x);
            }
            else {
                super.setObject(parameterIndex, x, targetSqlType);
            }
        }

        @Override
        public void setObject(int parameterIndex, Object x) throws SQLException {
            if (metaData.getParameterType(parameterIndex) == Types.BIGINT && x != null
                    && x instanceof Long) {
                setLong(parameterIndex, (Long) x);
            }
            else {
                super.setObject(parameterIndex, x);
            }
        }

        // todo How do we intercept this?
        @Override
        public void setObject(String parameterName, Object x, int targetSqlType, int scale)
                throws SQLException {
            super.setObject(parameterName, x, targetSqlType, scale);
        }

        // todo How do we intercept this?
        @Override
        public void setObject(String parameterName, Object x, int targetSqlType)
                throws SQLException {
            super.setObject(parameterName, x, targetSqlType);
        }

        // todo How do we intercept this?
        @Override
        public void setObject(String parameterName, Object x) throws SQLException {
            super.setObject(parameterName, x);
        }
    }
}
