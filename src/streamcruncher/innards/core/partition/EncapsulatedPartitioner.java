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
 * Author: Ashwin Jayaprakash Date: Feb 12, 2006 Time: 3:30:36 PM
 */

public class EncapsulatedPartitioner extends
        StreamReaderMemWriterPartitioner<EncapsulatedPartitionedTable> {
    protected ChainedPartitioner chainedTablePartitioner;

    // ----------------------

    @Override
    public void init(String queryName, EncapsulatedPartitionedTable table) throws Exception {
        super.init(queryName, table);

        ChainedPartitionedTable chainedPartitionedTable = table.getNextCPT();

        if (chainedPartitionedTable instanceof InMemChainedPartitionedTable) {
            chainedTablePartitioner = new InMemChainedPartitioner();
        }
        else {
            chainedTablePartitioner = new ChainedPartitioner();
        }

        chainedTablePartitioner.init(queryName, chainedPartitionedTable);

        PartitionOutputMemStore memStore = (PartitionOutputMemStore) storage;
        chainedTablePartitioner.initSourceMemoryStore(memStore);
    }

    /**
     * @return the chainedTablePartitioner
     */
    public ChainedPartitioner getChainedTablePartitioner() {
        return chainedTablePartitioner;
    }

    // --------------------

    @Override
    public void filter(QueryContext context) throws Exception {
        super.filter(context);

        chainedTablePartitioner.filter(context);
    }
}
