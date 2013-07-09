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
import java.io.Serializable;

import streamcruncher.api.artifact.RowSpec;
import streamcruncher.api.artifact.TableFQN;
import streamcruncher.innards.core.EventBucketClient;
import streamcruncher.innards.core.FilterInfo;
import streamcruncher.util.TwoDAppendOnlyList;

/*
 * Author: Ashwin Jayaprakash Date: Mar 26, 2006 Time: 2:54:51 PM
 */

public class ChainedPartitionedTable implements FilterInfo, Serializable {
    private static final long serialVersionUID = 1L;

    protected final TableFQN sourceTableFQN;

    protected final RowSpec sourceTableRowSpec;

    protected final String idColumnName;

    protected final TableFQN finalTableFQN;

    protected final PartitionSpec partitionSpec;

    /**
     * <code>null</code>, if this is the last in the chain.
     */
    protected ChainedPartitionedTable nextCPT;

    /**
     * @param sourceTableFQN
     * @param sourceTableRowSpec
     * @param finalTableFQN
     * @param partitionSpec
     */
    public ChainedPartitionedTable(TableFQN sourceTableFQN, RowSpec sourceTableRowSpec,
            TableFQN finalTableFQN, PartitionSpec partitionSpec) {
        this(sourceTableFQN, sourceTableRowSpec, finalTableFQN, partitionSpec, null);
    }

    /**
     * Used for Serialization.
     * 
     * @param sourceTableFQN
     * @param sourceTableRowSpec
     * @param finalTableFQN
     * @param partitionSpec
     * @param nextCPT
     */
    protected ChainedPartitionedTable(TableFQN sourceTableFQN, RowSpec sourceTableRowSpec,
            TableFQN finalTableFQN, PartitionSpec partitionSpec, ChainedPartitionedTable nextCPT) {
        this.sourceTableFQN = sourceTableFQN;
        this.sourceTableRowSpec = sourceTableRowSpec;
        this.finalTableFQN = finalTableFQN;
        this.partitionSpec = partitionSpec;

        String[] columnNames = sourceTableRowSpec.getColumnNames();
        this.idColumnName = columnNames[sourceTableRowSpec.getIdColumnPosition()];

        this.nextCPT = nextCPT;
    }

    protected Object writeReplace() throws ObjectStreamException {
        return new ChainedPartitionedTable(sourceTableFQN, sourceTableRowSpec, finalTableFQN,
                partitionSpec, nextCPT);
    }

    public void init() {
    }

    /**
     * @return Returns the finalTableFQN.
     */
    public TableFQN getTargetTableFQN() {
        return finalTableFQN;
    }

    /**
     * @param nextCPT
     *            The nextCPT to set.
     */
    public void setNextCPT(ChainedPartitionedTable nextCPT) {
        this.nextCPT = nextCPT;
    }

    /**
     * @return Returns the nextCPT.
     */
    public ChainedPartitionedTable getNextCPT() {
        return nextCPT;
    }

    /**
     * @return Returns the sourceTableFQN.
     */
    public TableFQN getSourceTableFQN() {
        return sourceTableFQN;
    }

    public String getIdColumnName() {
        return idColumnName;
    }

    // -----------

    public PartitionSpec getFilterSpec() {
        return partitionSpec;
    }

    public void eventsReceived() {
        // Ignore.
    }

    public void setStreamDataBuffer(TwoDAppendOnlyList list) {
        // Ignore.
    }

    public void setEventBucketClient(EventBucketClient eventBucketClient) {
        // Ignore.
    }

    public int getNumEventsInBucket() {
        // Ignore.
        return 0;
    }

    public boolean checkEquivalence(Object obj) {
        if ((obj instanceof ChainedPartitionedTable) == false) {
            return false;
        }

        ChainedPartitionedTable that = (ChainedPartitionedTable) obj;
        boolean result = that.sourceTableFQN.checkEquivalence(this.sourceTableFQN);
        result = result && that.sourceTableRowSpec.equals(this.sourceTableRowSpec);

        if (partitionSpec.getParameters() != null && that.partitionSpec.getParameters() != null) {
            result = result
                    && that.partitionSpec.getParameters()
                            .equals(this.partitionSpec.getParameters());
        }
        if (partitionSpec.getParameters() == null ^ that.partitionSpec.getParameters() == null) {
            result = result && false;
        }

        if (partitionSpec.getWhereClauseSpec() != null
                && that.partitionSpec.getWhereClauseSpec() != null) {
            result = result
                    && that.partitionSpec.getWhereClauseSpec().equals(
                            this.partitionSpec.getWhereClauseSpec());
        }
        if (partitionSpec.getWhereClauseSpec() == null
                ^ that.partitionSpec.getWhereClauseSpec() == null) {
            result = result && false;
        }

        return result;
    }

    public int equivalenceCode() {
        int hash = 0;
        if (partitionSpec.getParameters() != null) {
            hash = partitionSpec.getParameters().hashCode();
        }
        if (partitionSpec.getWhereClauseSpec() != null) {
            hash = (hash * 37) + partitionSpec.getWhereClauseSpec().hashCode();
        }
        hash = (hash * 37) + sourceTableFQN.equivalenceCode();
        hash = (hash * 37) + sourceTableRowSpec.hashCode();

        return hash;
    }
}
