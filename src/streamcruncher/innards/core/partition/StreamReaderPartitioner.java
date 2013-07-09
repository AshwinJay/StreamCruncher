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

import streamcruncher.boot.Registry;
import streamcruncher.innards.core.QueryContext;
import streamcruncher.innards.db.DatabaseInterface;
import streamcruncher.util.PerpetualResultSet;

/*
 * Author: Ashwin Jayaprakash Date: Feb 12, 2006 Time: 3:30:36 PM
 */

public abstract class StreamReaderPartitioner<P extends PartitionedTable> extends Partitioner<P> {
    protected PartitionedTable partitionedTable;

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

        PartitionOutputStore store = createStore();
        initStorage(store);
    }

    protected abstract PartitionOutputStore createStore();

    // -------------

    @Override
    protected int copyAndDescend(QueryContext context) throws Exception {
        int copied = 0;

        int rows = partitionedTable.forceFilter();
        if (rows > 0) {
            PerpetualResultSet resultSet = null;

            try {
                resultSet = partitionedTable.getPerpetualResultSet();
                int maxRows = resultSet.getSize();
                resultSet.startSession(maxRows);

                copied = partitionDescender.descendAndAddRows(context, resultSet);
            }
            finally {
                resultSet.closeSession();
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
