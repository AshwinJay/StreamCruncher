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
package streamcruncher.innards.core.filter;

import java.io.ObjectStreamException;
import java.io.Serializable;

import streamcruncher.api.artifact.RowSpec;
import streamcruncher.api.artifact.TableFQN;
import streamcruncher.innards.core.WhereClauseSpec;

/*
 * Author: Ashwin Jayaprakash Date: Feb 2, 2006 Time: 9:35:30 PM
 */

public class FilteredTable extends PreFilter {
    private static final long serialVersionUID = 1L;

    protected final TableFQN sourceTableFQN;

    protected final RowSpec sourceTableRowSpec;

    protected final String idColumnName;

    protected final TableFQN finalTableFQN;

    protected final String tableFilterClassName;

    // --------------------------

    protected final int hashCode;

    /**
     * @param queryName
     * @param realTableFQN
     *            The original Table that was being referred to, in the Query.
     * @param realTableRowSpec
     * @param finalTableFQN
     *            The new Table, which Copy of the first Table, except all the
     *            columns are moved one place right with a new Id-Column at the
     *            first position. This has been created by the Kernel for this
     *            Filter to write the filtered data into. This Table will be
     *            used by the Query to run the Query against, instead of the
     *            original one.
     * @param tableFilterFQN
     *            FQN of the Class implementing {@link TableFilter}.
     * @param spec
     */
    public FilteredTable(String queryName, TableFQN realTableFQN, RowSpec realTableRowSpec,
            TableFQN finalTableFQN, String tableFilterFQN, FilterSpec spec) {
        super(queryName, spec);

        this.sourceTableFQN = realTableFQN;
        this.sourceTableRowSpec = realTableRowSpec;
        this.idColumnName = sourceTableRowSpec.getColumnNames()[sourceTableRowSpec
                .getIdColumnPosition()];

        this.finalTableFQN = finalTableFQN;
        this.tableFilterClassName = tableFilterFQN;

        // --------------------------

        int hash = (realTableFQN + "").hashCode();
        hash = hash + (37 * (finalTableFQN + "").hashCode());
        hash = hash + (37 * (tableFilterClassName + "").hashCode());

        WhereClauseSpec whereClauseSpec = spec.getWhereClauseSpec();
        if (whereClauseSpec != null) {
            hash = hash + (37 * whereClauseSpec.getWhereClause().hashCode());
        }

        Serializable params = spec.getParameters();
        if (params != null) {
            hash = hash + (37 * (params + "").hashCode());
        }

        this.hashCode = hash;
    }

    @Override
    protected Object writeReplace() throws ObjectStreamException {
        return new FilteredTable(queryName, sourceTableFQN, sourceTableRowSpec, finalTableFQN,
                tableFilterClassName, filterSpec);
    }

    @Override
    public String getIdColumnName() {
        return idColumnName;
    }

    @Override
    public TableFQN getSourceTableFQN() {
        return sourceTableFQN;
    }

    @Override
    public RowSpec getSourceTableRowSpec() {
        return sourceTableRowSpec;
    }

    /**
     * @return Returns the finalTableFQN.
     */
    @Override
    public TableFQN getTargetTableFQN() {
        return finalTableFQN;
    }

    /**
     * @return Returns the tableFilterClassName.
     */
    public String getTableFilterClassName() {
        return tableFilterClassName;
    }

    // -----------------------

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof FilteredTable) {
            FilteredTable that = (FilteredTable) obj;

            boolean sameTables = sourceTableFQN.equals(that.getSourceTableFQN());
            sameTables = sameTables && finalTableFQN.equals(that.getTargetTableFQN());

            return sameTables && tableFilterClassName.equals(that.getTableFilterClassName());
        }

        return false;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    public boolean checkEquivalence(Object obj) {
        if ((obj instanceof FilteredTable) == false) {
            return false;
        }

        FilteredTable that = (FilteredTable) obj;
        boolean result = that.sourceTableFQN.checkEquivalence(this.sourceTableFQN);
        result = result && that.sourceTableRowSpec.equals(this.sourceTableRowSpec);

        if (filterSpec.parameters != null && that.filterSpec.parameters != null) {
            result = result && that.filterSpec.parameters.equals(this.filterSpec.parameters);
        }
        if (filterSpec.parameters == null ^ that.filterSpec.parameters == null) {
            result = result && false;
        }

        if (filterSpec.whereClauseSpec != null && that.filterSpec.whereClauseSpec != null) {
            result = result
                    && that.filterSpec.whereClauseSpec.equals(this.filterSpec.whereClauseSpec);
        }
        if (filterSpec.whereClauseSpec == null ^ that.filterSpec.whereClauseSpec == null) {
            result = result && false;
        }

        return result;
    }

    public int equivalenceCode() {
        int hash = 0;
        if (filterSpec.parameters != null) {
            hash = filterSpec.parameters.hashCode();
        }
        if (filterSpec.whereClauseSpec != null) {
            hash = (hash * 37) + filterSpec.whereClauseSpec.hashCode();
        }
        hash = (hash * 37) + sourceTableFQN.equivalenceCode();
        hash = (hash * 37) + sourceTableRowSpec.hashCode();

        return hash;
    }
}
