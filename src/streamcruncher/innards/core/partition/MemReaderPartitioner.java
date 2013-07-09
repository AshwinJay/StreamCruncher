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

import java.util.Collection;

import streamcruncher.api.artifact.RowSpec;
import streamcruncher.boot.Registry;
import streamcruncher.innards.core.QueryContext;
import streamcruncher.innards.core.WhereClauseSpec;
import streamcruncher.innards.db.DatabaseInterface;
import streamcruncher.innards.impl.expression.OgnlRowEvaluator;
import streamcruncher.util.PerpetualResultSet;
import streamcruncher.util.RowEvaluator;

/*
 * Author: Ashwin Jayaprakash Date: Feb 12, 2006 Time: 3:30:36 PM
 */

/**
 * Can consume only New or Dead Rows.
 */
public abstract class MemReaderPartitioner<P extends ChainedPartitionedTable> extends
        Partitioner<P> {
    protected ChainedPartitionedTable partitionedTable;

    protected PartitionOutputMemStore sourceMemStore;

    protected PerpetualResultSet perpetualResultSet;

    protected RowStatus rowComsumptionStatus;

    protected RowEvaluator eventFilter;

    protected DatabaseInterface dbInterface;

    // -------------

    /**
     * @param queryName
     * @param partitionedTable
     * @throws Exception
     */
    @Override
    public void init(String queryName, P partitionedTable) throws Exception {
        super.init(queryName, partitionedTable);

        this.partitionedTable = partitionedTable;

        dbInterface = Registry.getImplFor(DatabaseInterface.class);

        PartitionSpec partitionSpec = partitionedTable.getFilterSpec();
        RowSpec sourceRowSpec = partitionSpec.getSourceTableRowSpec();
        String[] columns = sourceRowSpec.getColumnNames();
        perpetualResultSet = new PerpetualResultSet(columns);

        WhereClauseSpec whereClauseSpec = partitionSpec.getWhereClauseSpec();
        rowComsumptionStatus = (RowStatus) whereClauseSpec.getContext().get(
                RowStatus.class.getName());
        String clause = whereClauseSpec.getWhereClause();
        if (clause != null && clause.length() > 0) {
            eventFilter = new OgnlRowEvaluator(queryName, clause, sourceRowSpec, whereClauseSpec
                    .getContext(), whereClauseSpec.getSubQueries());
        }

        PartitionOutputStore store = createStore();
        initStorage(store);
    }

    protected abstract PartitionOutputStore createStore();

    protected void initSourceMemoryStore(PartitionOutputMemStore memStore) {
        sourceMemStore = memStore;
    }

    // -------------

    @Override
    protected int copyAndDescend(QueryContext context) throws Exception {
        int copied = 0;

        Collection<Object[]> collection = null;
        if (rowComsumptionStatus == RowStatus.NEW) {
            collection = sourceMemStore.retrieveNewRowsInBatch();
        }
        else if (rowComsumptionStatus == RowStatus.DEAD) {
            collection = sourceMemStore.retrieveDeadRowsInBatch();
        }

        if (collection != null && collection.size() > 0) {
            try {
                perpetualResultSet.pumpRows(collection, eventFilter);
                perpetualResultSet.startSession(perpetualResultSet.getSize());

                copied = partitionDescender.descendAndAddRows(context, perpetualResultSet);
            }
            finally {
                perpetualResultSet.closeSession();
            }
        }

        return copied;
    }

    protected void postProcess(QueryContext context, int rowsCopied, int rowsOusted,
            int rowsInserted) {
    }

    // -------------

    @Override
    public void discard() {
        super.discard();

        partitionedTable = null;
    }
}
