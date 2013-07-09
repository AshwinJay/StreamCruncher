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
package streamcruncher.innards.core.partition;

import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import streamcruncher.api.DBName;
import streamcruncher.api.artifact.RowSpec;
import streamcruncher.boot.Registry;
import streamcruncher.innards.core.QueryContext;
import streamcruncher.innards.db.Constants;
import streamcruncher.innards.db.DatabaseInterface;
import streamcruncher.innards.util.Helper;
import streamcruncher.util.AppendOnlyPrimitiveLongList;

/*
 * Author: Ashwin Jayaprakash Date: Jun 23, 2007 Time: 7:39:32 PM
 */

public class PartitionOutputTableStore extends PartitionOutputStore {
    protected final int rowIdPosition;

    protected final int versionIdPosition;

    protected DBName dbName;

    protected String markForDeleteSQL;

    protected String deleteSQL;

    protected String insertSQL;

    protected Map<Integer, Integer> insertColPosAndSQLTypeMap;

    private Connection connection;

    public PartitionOutputTableStore(streamcruncher.innards.core.FilterInfo filterInfo) {
        super(filterInfo);

        this.rowIdPosition = Constants.ID_COLUMN_POS;
        this.versionIdPosition = Constants.VERSION_COLUMN_POS;

        DatabaseInterface dbInterface = Registry.getImplFor(DatabaseInterface.class);
        this.dbName = dbInterface.getDBName();

        this.insertSQL = createInsertSQL(spec);
        this.markForDeleteSQL = createMarkForDeleteSQL(spec);
        this.deleteSQL = createDeleteSQL(spec);

        this.insertColPosAndSQLTypeMap = new HashMap<Integer, Integer>();
    }

    protected String createInsertSQL(PartitionSpec spec) {
        RowSpec rowSpec = spec.getFunctionBuilder().getFinalTableRowSpec();
        String[] cols = rowSpec.getColumnNames();

        StringBuilder builder = new StringBuilder();

        builder.append("insert into ");
        builder.append(filterInfo.getTargetTableFQN().getFQN());
        builder.append("(");

        for (int i = 0; i < cols.length; i++) {
            builder.append(cols[i]);

            if (i < cols.length - 1) {
                builder.append(", ");
            }
        }
        builder.append(") values(");
        for (int i = 0; i < cols.length; i++) {
            builder.append("?");

            if (i < cols.length - 1) {
                builder.append(", ");
            }
        }
        builder.append(")");

        return builder.toString();
    }

    protected String createMarkForDeleteSQL(PartitionSpec spec) {
        String[] columnNames = spec.getFunctionBuilder().getFinalTableRowSpec().getColumnNames();

        return "update " + filterInfo.getTargetTableFQN().getFQN() + " set "
                + columnNames[Constants.VERSION_COLUMN_POS] + " = ? where "
                + columnNames[Constants.ID_COLUMN_POS] + " = ?";
    }

    protected String createDeleteSQL(PartitionSpec spec) {
        String[] columnNames = spec.getFunctionBuilder().getFinalTableRowSpec().getColumnNames();

        return "delete from " + filterInfo.getTargetTableFQN().getFQN() + " where "
                + columnNames[Constants.VERSION_COLUMN_POS] + " < 0 and "
                + columnNames[Constants.VERSION_COLUMN_POS] + " > ?";
    }

    // -------------

    @Override
    public void startBatch(QueryContext context) throws Exception {
        super.startBatch(context);

        connection = context.createConnection();
        connection.setAutoCommit(false);
        connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
    }

    @Override
    public void insertNewRow(QueryContext context, List<Row> rows) throws SQLException {
        PreparedStatement statement = null;
        int inserted = 0;

        try {
            statement = connection.prepareStatement(insertSQL);

            int length = rows.size();
            for (int i = 0; i < length; i++) {
                Row row = rows.remove(0);
                Object[] columns = row.getColumns();
                setValues(statement, columns);
                statement.addBatch();
            }

            int[] results = statement.executeBatch();
            for (int i : results) {
                if (i != Statement.SUCCESS_NO_INFO && i != Statement.EXECUTE_FAILED) {
                    inserted = inserted + i;
                }
            }
        }
        finally {
            Helper.closeStatement(statement);
        }
    }

    @Override
    public void markRowsAsDead(QueryContext context, long markValue,
            AppendOnlyPrimitiveLongList idList) throws SQLException {
        PreparedStatement statement = null;

        try {
            statement = connection.prepareStatement(markForDeleteSQL);

            int length = idList.getSize();
            for (int i = 0; i < length; i++) {
                long rowId = idList.remove();
                statement.setLong(1, markValue);
                statement.setLong(2, rowId);

                statement.addBatch();
            }

            int[] results = statement.executeBatch();
        }
        finally {
            Helper.closeStatement(statement);
        }
    }

    protected void setValues(PreparedStatement statement, Object[] columns) throws SQLException {
        for (int j = 0; j < columns.length; j++) {
            if (columns[j] != null) {
                statement.setObject(j + 1, columns[j]);
            }
            else {
                /*
                 * doc Derby complains if a null Object is set directly using
                 * setObject(xx).
                 */
                if (dbName == DBName.Derby) {
                    int pos = j + 1;
                    Integer type = insertColPosAndSQLTypeMap.get(pos);

                    if (type == null) {
                        ParameterMetaData metaData = statement.getParameterMetaData();

                        type = metaData.getParameterType(pos);

                        insertColPosAndSQLTypeMap.put(pos, type);
                    }

                    statement.setNull(pos, type.intValue());
                }
                else {
                    statement.setObject(j + 1, columns[j]);
                }
            }
        }
    }

    @Override
    public void deleteDeadRows(QueryContext context) throws SQLException {
        PreparedStatement statement = null;

        try {
            statement = connection.prepareStatement(deleteSQL);

            long deleteId = -1 * context.getRunCount();
            statement.setLong(1, deleteId);

            int results = statement.executeUpdate();
        }
        finally {
            Helper.closeStatement(statement);
        }
    }

    @Override
    public void endBatch(QueryContext context, boolean successfulEnd) throws Exception {
        try {
            super.endBatch(context, successfulEnd);
        }
        finally {
            if (connection != null) {
                if (successfulEnd) {
                    connection.commit();
                }
                else {
                    // todo There is no real rollback.
                    connection.rollback();
                }
            }

            Helper.closeConnection(connection);
            connection = null;
        }
    }

    @Override
    public void discard() {
    }
}