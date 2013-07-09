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
package streamcruncher.innards.core.stream;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import streamcruncher.api.artifact.RowSpec;
import streamcruncher.api.artifact.TableSpec;
import streamcruncher.boot.Registry;
import streamcruncher.innards.db.DatabaseInterface;
import streamcruncher.innards.util.Helper;
import streamcruncher.kernel.DBQueryOutput;
import streamcruncher.kernel.DirectQueryOutput;
import streamcruncher.kernel.QueryOutput;

/*
 * Author: Ashwin Jayaprakash Date: Jan 19, 2006 Time: 1:32:36 PM
 */

public class OutStream {
    protected final String schema;

    protected final String name;

    protected final String resultTblIdColumn;

    protected final int totalResultColumns;

    protected final String queryName;

    protected final LinkedBlockingQueue<QueryOutput> queueOfIdPairs;

    protected DatabaseInterface dbInterface;

    protected String resultFetchetSQL;

    protected String deleteSQL;

    // -------------

    protected final int hashCode;

    protected final String str;

    // -------------

    /**
     * @param schema
     * @param name
     * @param queryName
     */
    public OutStream(String queryName, TableSpec resultTableSpec) {
        this.schema = resultTableSpec.getSchema();
        this.name = resultTableSpec.getName();

        RowSpec spec = resultTableSpec.getRowSpec();
        String[] columns = spec.getColumnNames();
        // Auto generated Id column for the Output Stream.
        this.resultTblIdColumn = columns[spec.getIdColumnPosition()];
        this.totalResultColumns = columns.length;

        this.queryName = queryName;
        this.queueOfIdPairs = new LinkedBlockingQueue<QueryOutput>();

        // -------------

        int hash = (schema + ".").hashCode();
        hash = hash + (37 * (name + " ").hashCode());
        hash = hash + (37 * (queryName + " ").hashCode());
        this.hashCode = hash;

        this.str = schema + "." + name + "-" + queryName;

        // --------------

        init();
    }

    /**
     * @return the queryName
     */
    public String getQueryName() {
        return queryName;
    }

    // -------------

    protected void init() {
        dbInterface = Registry.getImplFor(DatabaseInterface.class);
        resultFetchetSQL = getResultFetcherSQL();
        deleteSQL = getDeleteSQL();
    }

    protected String getResultFetcherSQL() {
        String tbl = (schema == null) ? name : (schema + "." + name);

        return "select * from " + tbl + " where " + resultTblIdColumn + " > ? and "
                + resultTblIdColumn + " <= ?";
    }

    protected String getDeleteSQL() {
        String tbl = (schema == null) ? name : (schema + "." + name);
        return "delete from " + tbl + " where " + resultTblIdColumn + " <= ?";
    }

    // -------------

    /**
     * @param queryOutputInfo
     */
    public void addNextWindowRange(QueryOutput queryOutput) {
        queueOfIdPairs.add(queryOutput);
    }

    /**
     * Blocks until at least one Event is available.
     * 
     * @return
     * @throws SQLException
     * @throws InterruptedException
     */
    public List<Object[]> takeEvents() throws SQLException, InterruptedException {
        QueryOutput output = queueOfIdPairs.take();

        List<Object[]> events = null;
        if (output instanceof DBQueryOutput) {
            events = takeEvents((DBQueryOutput) output);
        }
        else {
            events = takeEvents((DirectQueryOutput) output);
        }

        return events;
    }

    /**
     * @param timeout
     * @param timeUnit
     * @return Empty array if the operation timed out.
     * @throws InterruptedException
     * @throws SQLException
     */
    public List<Object[]> takeEvents(long timeout, TimeUnit timeUnit) throws InterruptedException,
            SQLException {
        List<Object[]> events = null;

        QueryOutput output = queueOfIdPairs.poll(timeout, timeUnit);
        if (output != null) {
            if (output instanceof DBQueryOutput) {
                events = takeEvents((DBQueryOutput) output);
            }
            else {
                events = takeEvents((DirectQueryOutput) output);
            }
        }
        else {
            events = new ArrayList<Object[]>();
        }

        return events;
    }

    protected List<Object[]> takeEvents(DirectQueryOutput output) {
        return output.getOutput();
    }

    protected List<Object[]> takeEvents(DBQueryOutput output) throws SQLException {
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;

        ArrayList<Object[]> rows = new ArrayList<Object[]>(output.getRows());

        try {
            connection = dbInterface.createConnection();
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);

            statement = connection.prepareStatement(deleteSQL);
            statement.setLong(1, output.getStartIdExclusive());
            statement.execute();
            Helper.closeStatement(statement);

            // -------------

            statement = connection.prepareStatement(resultFetchetSQL);
            statement.setLong(1, output.getStartIdExclusive());
            statement.setLong(2, output.getEndIdInclusive());

            resultSet = statement.executeQuery();

            int resultColumns = totalResultColumns - 1;
            while (resultSet.next()) {
                Object[] row = new Object[resultColumns];
                for (int i = 2 /* Skip the first auto-id column. */; i <= totalResultColumns; i++) {
                    row[i - 2] = resultSet.getObject(i);
                }
                rows.add(row);
            }
        }
        finally {
            Helper.closeResultSet(resultSet);
            Helper.closeStatement(statement);
            Helper.closeConnection(connection);
        }

        return rows;
    }

    // -------------

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof OutStream) {
            OutStream that = (OutStream) obj;

            String thisStr = toString();
            String thatStr = that.toString();

            return thisStr.equals(thatStr);
        }

        return false;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public String toString() {
        return str;
    }
}
