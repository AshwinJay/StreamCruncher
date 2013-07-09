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

import java.io.ObjectStreamException;

import streamcruncher.api.artifact.RowSpec;
import streamcruncher.api.artifact.TableFQN;

/*
 * Author: Ashwin Jayaprakash Date: Mar 26, 2006 Time: 2:54:51 PM
 */

public class EncapsulatedPartitionedTable extends PartitionedTable {
    private static final long serialVersionUID = 1L;

    protected ChainedPartitionedTable nextCPT;

    /**
     * @param queryName
     * @param realTableFQN
     * @param realTableRowSpec
     * @param finalTableFQN
     * @param partitionSpec
     */
    public EncapsulatedPartitionedTable(String queryName, TableFQN realTableFQN,
            RowSpec realTableRowSpec, TableFQN finalTableFQN, PartitionSpec partitionSpec) {
        this(queryName, realTableFQN, realTableRowSpec, finalTableFQN, partitionSpec, null);
    }

    /**
     * Used for Serialization.
     * 
     * @param queryName
     * @param realTableFQN
     * @param realTableRowSpec
     * @param finalTableFQN
     * @param partitionSpec
     * @param nextCPT
     */
    protected EncapsulatedPartitionedTable(String queryName, TableFQN realTableFQN,
            RowSpec realTableRowSpec, TableFQN finalTableFQN, PartitionSpec partitionSpec,
            ChainedPartitionedTable nextCPT) {
        super(queryName, realTableFQN, realTableRowSpec, finalTableFQN,
                EncapsulatedPartitioner.class.getName(), partitionSpec);

        this.nextCPT = nextCPT;
    }

    @Override
    protected Object writeReplace() throws ObjectStreamException {
        return new EncapsulatedPartitionedTable(queryName, sourceTableFQN, sourceTableRowSpec,
                finalTableFQN, (PartitionSpec) filterSpec, nextCPT);
    }

    /**
     * @return Returns the nextCPT.
     */
    public ChainedPartitionedTable getNextCPT() {
        return nextCPT;
    }

    /**
     * @param nextCPT
     *            The nextCPT to set.
     */
    public void setNextCPT(ChainedPartitionedTable nextPartitionedTable) {
        this.nextCPT = nextPartitionedTable;
    }
}
