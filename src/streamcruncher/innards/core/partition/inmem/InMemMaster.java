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
package streamcruncher.innards.core.partition.inmem;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import streamcruncher.api.artifact.RowSpec;
import streamcruncher.innards.core.WhereClauseSpec;
import streamcruncher.innards.core.filter.TableFilter;
import streamcruncher.innards.core.partition.Row;
import streamcruncher.innards.core.partition.RowStatus;
import streamcruncher.innards.expression.Statement;
import streamcruncher.innards.impl.expression.ExpressionEvaluationException;
import streamcruncher.innards.impl.expression.OgnlRowEvaluator;
import streamcruncher.util.AppendOnlyPrimitiveLongList;

/*
 * Author: Ashwin Jayaprakash Date: Sep 4, 2007 Time: 8:15:49 PM
 */

public class InMemMaster {
    protected final InMemSpec memSpec;

    protected final InMemPartitionDataProducer dataProducer;

    protected final OgnlRowEvaluator rowFilter;

    protected final OgnlRowEvaluator[] columnEvaluators;

    protected final int partitionTableRowIdPosition;

    protected final HashMap<Long, Object[]> rowIdAndDataMap;

    protected final RowStatus rowStatus;

    public InMemMaster(String queryName, InMemSpec memSpec, TableFilter source)
            throws ExpressionEvaluationException {
        this.memSpec = memSpec;

        List<InMemPartitionDataProducer> producerList = Util
                .convertToList(new TableFilter[] { source });
        this.dataProducer = producerList.get(0);

        // ---------------

        OgnlRowEvaluator eventFilter = null;
        RowSpec rowSpec = memSpec.getPartitionTableSpec().getRowSpec();

        RowStatus tmpRowStatus = null;

        WhereClauseSpec whereClauseSpec = this.memSpec.getWhereClauseSpec();
        String whereClauseStr = null;
        if (whereClauseSpec != null) {
            whereClauseStr = whereClauseSpec.getWhereClause();

            if (whereClauseStr != null && whereClauseStr.length() > 0) {
                eventFilter = new OgnlRowEvaluator(queryName, whereClauseStr, rowSpec,
                        whereClauseSpec.getContext(), whereClauseSpec.getSubQueries());
            }

            tmpRowStatus = (RowStatus) whereClauseSpec.getContext().get(RowStatus.class.getName());
        }

        this.rowFilter = eventFilter;
        this.rowStatus = tmpRowStatus;

        // ---------------

        Statement statement = memSpec.getStatement();
        List<WhereClauseSpec> columnExpressions = statement.getColumnExpressions();

        // Create a shared Context first.
        Map<String, Object> commonSharedContext = new HashMap<String, Object>();
        for (WhereClauseSpec spec : columnExpressions) {
            commonSharedContext.putAll(spec.getContext());
        }

        this.columnEvaluators = new OgnlRowEvaluator[columnExpressions.size()];
        int x = 0;
        for (WhereClauseSpec spec : columnExpressions) {
            this.columnEvaluators[x++] = new OgnlRowEvaluator(queryName, spec.getWhereClause(),
                    rowSpec, commonSharedContext, spec.getSubQueries());
        }

        // ---------------

        this.partitionTableRowIdPosition = memSpec.getPartitionTableSpec().getRowSpec()
                .getIdColumnPosition();
        this.rowIdAndDataMap = new HashMap<Long, Object[]>();
    }

    public List<Object[]> onCycleEnd() throws ExpressionEvaluationException {
        List<Object[]> results = new LinkedList<Object[]>();

        AppendOnlyPrimitiveLongList removals = dataProducer.retrieveDeadRowIdsInBatch();
        if (removals != null) {
            long[] ids = removals.removeAvailable();
            while (ids.length > 0) {
                for (int i = 0; i < ids.length; i++) {
                    Object[] columns = rowIdAndDataMap.remove(ids[i]);

                    if (rowStatus == null || rowStatus == RowStatus.DEAD) {
                        results.add(columns);
                    }
                }

                ids = removals.removeAvailable();
            }
        }

        List<Row> rows = dataProducer.retrieveNewRowsInBatch();
        if (rows != null) {
            for (Row row : rows) {
                Object[] columns = row.getColumns();

                if (rowStatus != null && rowStatus == RowStatus.NEW) {
                    results.add(columns);
                }
                else {
                    Long id = (Long) columns[partitionTableRowIdPosition];
                    rowIdAndDataMap.put(id, columns);
                }
            }
        }

        if (rowStatus != null && rowStatus == RowStatus.NOT_DEAD) {
            for (Entry<Long, Object[]> entry : rowIdAndDataMap.entrySet()) {
                results.add(entry.getValue());
            }
        }

        List<Object[]> retVal = Util.filterRows(memSpec.getStatement(), rowFilter,
                columnEvaluators, results);
        return retVal;
    }
}
