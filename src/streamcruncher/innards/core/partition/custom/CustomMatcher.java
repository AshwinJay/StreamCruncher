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
package streamcruncher.innards.core.partition.custom;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import streamcruncher.api.CustomStore;
import streamcruncher.api.artifact.RowSpec;
import streamcruncher.innards.core.WhereClauseSpec;
import streamcruncher.innards.core.filter.TableFilter;
import streamcruncher.innards.core.partition.Row;
import streamcruncher.innards.core.partition.inmem.InMemPartitionDataProducer;
import streamcruncher.innards.core.partition.inmem.Util;
import streamcruncher.innards.expression.Statement;
import streamcruncher.innards.impl.expression.ExpressionEvaluationException;
import streamcruncher.innards.impl.expression.OgnlRowEvaluator;
import streamcruncher.util.AppendOnlyPrimitiveLongList;

/*
 * Author: Ashwin Jayaprakash Date: Feb 23, 2007 Time: 8:58:38 PM
 */

public class CustomMatcher {
    protected final CustomSpec customSpec;

    protected final OgnlRowEvaluator[] columnEvaluators;

    protected final InMemPartitionDataProducer[] sources;

    protected final Map<String, Map<Long, Object[]>> aliasToIdDataMap;

    protected final CustomStore customStore;

    public CustomMatcher(String queryName, CustomSpec CustomSpec, TableFilter[] sources)
            throws ExpressionEvaluationException, ClassNotFoundException, InstantiationException,
            IllegalAccessException {
        this.customSpec = CustomSpec;

        Class clazz = Class.forName(this.customSpec.getCustomStoreClassFQN());
        customStore = (CustomStore) clazz.newInstance();

        // ---------------

        Statement statement = CustomSpec.getStatement();
        List<WhereClauseSpec> columnExpressions = statement.getColumnExpressions();

        // Create a shared Context first.
        Map<String, Object> commonSharedContext = new HashMap<String, Object>();
        for (WhereClauseSpec spec : columnExpressions) {
            commonSharedContext.putAll(spec.getContext());
        }

        this.columnEvaluators = new OgnlRowEvaluator[columnExpressions.size()];
        int x = 0;
        for (WhereClauseSpec spec : columnExpressions) {
            // ??? Multiple RowSpecs.
            // this.columnEvaluators[x++] = new OgnlRowEvaluator(queryName,
            // spec.getWhereClause(),
            // rowSpec, commonSharedContext, spec.getSubQueries());
        }

        // ---------------

        List<InMemPartitionDataProducer> corrList = Util.convertToList(sources);
        this.sources = corrList.toArray(new InMemPartitionDataProducer[corrList.size()]);

        this.aliasToIdDataMap = new HashMap<String, Map<Long, Object[]>>();
        String[] aliasOrFQNs = new String[this.sources.length];
        for (int i = 0; i < this.sources.length; i++) {
            String s = this.sources[i].getTargetTableFQN().getAliasOrFQN();
            this.aliasToIdDataMap.put(s, new HashMap<Long, Object[]>());
        }

        this.customStore.init(queryName, this.customSpec.getSourceTblAliasAndRowSpec(),
                this.customSpec.getWhereClause());
    }

    public List<Object[]> onCycleEnd() throws ExpressionEvaluationException {
        startBatch();

        // Removals first.
        for (InMemPartitionDataProducer source : sources) {
            String alias = source.getTargetTableFQN().getAliasOrFQN();

            AppendOnlyPrimitiveLongList removals = source.retrieveDeadRowIdsInBatch();
            if (removals != null) {
                long[] ids = removals.removeAvailable();
                while (ids.length > 0) {
                    for (int i = 0; i < ids.length; i++) {
                        Object[] data = aliasToIdDataMap.get(alias).get(ids[i]);
                        removed(alias, ids[i], data);
                    }

                    ids = removals.removeAvailable();
                }
            }
        }

        // --------------

        // Additions next.
        for (InMemPartitionDataProducer source : sources) {
            String alias = source.getTargetTableFQN().getAliasOrFQN();
            RowSpec rowSpec = customSpec.getSourceTblAliasAndRowSpec().get(alias);

            List<Row> rows = source.retrieveNewRowsInBatch();
            if (rows != null) {
                Row[] additions = rows.toArray(new Row[rows.size()]);
                int rowIdPosition = rowSpec.getIdColumnPosition();

                for (Row row : additions) {
                    Object[] data = row.getColumns();
                    Long rowId = ((Number) data[rowIdPosition]).longValue();
                    aliasToIdDataMap.get(alias).put(rowId, data);
                    added(alias, rowId, data);
                }
            }
        }

        // --------------

        // ???
        List<Object[]> list = endBatch();
        return list;

        // ???
        // return Util.filterRows(customSpec.getStatement(), null,
        // columnEvaluators);
    }

    public void startBatch() {
        customStore.startBatch();
    }

    public void added(String alias, Long id, Object[] data) {
        customStore.added(alias, id, data);
    }

    public void removed(String alias, Long id, Object[] data) {
        customStore.removed(alias, id, data);
    }

    public List<Object[]> endBatch() {
        return customStore.endBatch();
    }
}
