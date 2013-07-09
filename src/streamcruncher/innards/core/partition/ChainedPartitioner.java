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

import streamcruncher.innards.core.QueryContext;
import streamcruncher.innards.core.partition.inmem.InMemChainedPartitionedTable;
import streamcruncher.innards.core.partition.inmem.InMemChainedPartitioner;

/*
 * Author: Ashwin Jayaprakash Date: Mar 26, 2006 Time: 2:50:51 PM
 */

public class ChainedPartitioner extends MemReaderPartitioner<ChainedPartitionedTable> {
    protected ChainedPartitionedTable chainedPartitionedTable;

    protected ChainedPartitioner nextCTP;

    // -------------

    /**
     * @param queryName
     * @param chainedPartitionedTable
     * @throws Exception
     */
    @Override
    public void init(String queryName, ChainedPartitionedTable chainedPartitionedTable)
            throws Exception {
        this.chainedPartitionedTable = chainedPartitionedTable;

        super.init(queryName, chainedPartitionedTable);

        ChainedPartitionedTable nextCPT = chainedPartitionedTable.getNextCPT();
        if (nextCPT != null) {
            if (nextCPT instanceof InMemChainedPartitionedTable) {
                nextCTP = new InMemChainedPartitioner();
            }
            else {
                nextCTP = new ChainedPartitioner();
            }

            nextCTP.init(queryName, nextCPT);

            PartitionOutputMemStore memStore = (PartitionOutputMemStore) storage;
            nextCTP.initSourceMemoryStore(memStore);
        }
    }

    @Override
    protected PartitionOutputStore createStore() {
        if (chainedPartitionedTable.getNextCPT() != null) {
            return new PartitionOutputMemStore(chainedPartitionedTable);
        }

        return new PartitionOutputTableStore(chainedPartitionedTable);
    }

    /**
     * @return the nextCTP <code>null</code> if this is the last in the chain.
     */
    public ChainedPartitioner getNextCTP() {
        return nextCTP;
    }

    // -------------

    @Override
    public void filter(QueryContext context) throws Exception {
        super.filter(context);

        if (nextCTP != null) {
            nextCTP.filter(context);
        }
    }

    // -------------

    @Override
    public void discard() {
        super.discard();

        chainedPartitionedTable = null;
        if (nextCTP != null) {
            nextCTP.discard();
            nextCTP = null;
        }
    }
}