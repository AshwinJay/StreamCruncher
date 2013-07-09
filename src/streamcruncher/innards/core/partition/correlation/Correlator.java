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
package streamcruncher.innards.core.partition.correlation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import streamcruncher.api.artifact.RowSpec;
import streamcruncher.api.artifact.TableFQN;
import streamcruncher.innards.core.WhereClauseSpec;
import streamcruncher.innards.core.filter.TableFilter;
import streamcruncher.innards.core.partition.Row;
import streamcruncher.innards.core.partition.correlation.CorrelationSpec.MatchSpec;
import streamcruncher.innards.core.partition.inmem.InMemPartitionDataProducer;
import streamcruncher.innards.core.partition.inmem.Util;
import streamcruncher.innards.expression.Statement;
import streamcruncher.innards.impl.expression.ExpressionEvaluationException;
import streamcruncher.innards.impl.expression.OgnlRowEvaluator;
import streamcruncher.util.AppendOnlyPrimitiveLongList;

/*
 * Author: Ashwin Jayaprakash Date: Feb 23, 2007 Time: 8:58:38 PM
 */

public class Correlator {
    protected final CorrelationSpec correlationSpec;

    protected final OgnlRowEvaluator rowFilter;

    protected final OgnlRowEvaluator[] columnEvaluators;

    protected final TableFQN[] targetFQNs;

    protected final InMemPartitionDataProducer[] sources;

    /**
     * Source Table -> Row Id -> Correlation Id map.
     */
    protected final Map<String, Map<Long, Object>> sourceTblRowIdAndCorrIds;

    /**
     * Source Table -> Correlation Id -> Data.
     * <p>
     * If a new Row appears with the same Correlation while another one still
     * exists in the map, then the new one will just overwrite the old one.
     * </p>
     */
    protected final Map<String, Map<Object, Object[]>> sourceTblCorrIdAndData;

    /**
     * Map of all Matchers that a Source Table is used in.
     */
    protected final Map<String, Matcher[]> sourceTblAndMatchers;

    protected final Matcher[] matchers;

    protected final int totalResultTableColumns;

    public Correlator(String queryName, CorrelationSpec correlationSpec, TableFilter[] sources)
            throws ExpressionEvaluationException {
        this.correlationSpec = correlationSpec;

        OgnlRowEvaluator eventFilter = null;
        RowSpec rowSpec = correlationSpec.getOutputTableSpec().getRowSpec();

        WhereClauseSpec whereClauseSpec = this.correlationSpec.getWhereClauseSpec();
        if (whereClauseSpec != null) {
            eventFilter = new OgnlRowEvaluator(queryName, whereClauseSpec.getWhereClause(),
                    rowSpec, whereClauseSpec.getContext(), whereClauseSpec.getSubQueries());
        }
        this.rowFilter = eventFilter;

        // ---------------

        Statement statement = correlationSpec.getStatement();
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

        List<InMemPartitionDataProducer> corrList = Util.convertToList(sources);
        this.sources = corrList.toArray(new InMemPartitionDataProducer[corrList.size()]);

        this.sourceTblRowIdAndCorrIds = new HashMap<String, Map<Long, Object>>();
        this.sourceTblCorrIdAndData = new HashMap<String, Map<Object, Object[]>>();

        this.targetFQNs = new TableFQN[this.sources.length];
        for (int i = 0; i < sources.length; i++) {
            this.targetFQNs[i] = this.sources[i].getTargetTableFQN();

            this.sourceTblRowIdAndCorrIds.put(this.targetFQNs[i].getAlias(),
                    new HashMap<Long, Object>());
            this.sourceTblCorrIdAndData.put(this.targetFQNs[i].getAlias(),
                    new HashMap<Object, Object[]>());
        }

        // --------------

        int totalResultColumns = 0;
        Map<String, Integer[][]> srcTblAndDestPosition = this.correlationSpec
                .getSourceTblAndDestPosition();
        for (String key : srcTblAndDestPosition.keySet()) {
            Integer[][] positions = srcTblAndDestPosition.get(key);
            totalResultColumns = totalResultColumns + positions.length;
        }
        this.totalResultTableColumns = totalResultColumns;

        // --------------

        LinkedList<Matcher> allMatchers = new LinkedList<Matcher>();

        for (MatchSpec matchSpec : this.correlationSpec.getMatchSpecs()) {
            String[] present = matchSpec.getPresentAliases();
            String[] notPresent = matchSpec.getNotPresentAliases();

            Matcher matcher = null;

            if (notPresent.length == 0) {
                matcher = new ImmediateMatcher(present, notPresent);
            }
            else {
                matcher = new DelayedMatcher(present, notPresent);
            }

            allMatchers.add(matcher);
        }

        this.matchers = allMatchers.toArray(new Matcher[allMatchers.size()]);

        // --------------

        HashMap<String, List<Matcher>> aliasAndMatchers = new HashMap<String, List<Matcher>>();
        for (Matcher matcher : allMatchers) {
            String[] present = matcher.getPresentAliases();
            for (String alias : present) {
                List<Matcher> list = aliasAndMatchers.get(alias);
                if (list == null) {
                    list = new LinkedList<Matcher>();
                    aliasAndMatchers.put(alias, list);
                }
                list.add(matcher);
            }

            String[] notPresent = matcher.getNotPresentAliases();
            for (String alias : notPresent) {
                List<Matcher> list = aliasAndMatchers.get(alias);
                if (list == null) {
                    list = new LinkedList<Matcher>();
                    aliasAndMatchers.put(alias, list);
                }
                list.add(matcher);
            }
        }

        this.sourceTblAndMatchers = new HashMap<String, Matcher[]>();
        for (String alias : aliasAndMatchers.keySet()) {
            List<Matcher> list = aliasAndMatchers.get(alias);
            Matcher[] matcherArr = list.toArray(new Matcher[list.size()]);
            this.sourceTblAndMatchers.put(alias, matcherArr);
        }
    }

    public List<Object[]> onCycleEnd() throws ExpressionEvaluationException {
        LinkedList<Object[]> intermediateResults = new LinkedList<Object[]>();

        HashMap<String, ArrayList<Object>> aliasAndExpelledCorrIds = new HashMap<String, ArrayList<Object>>();

        for (Matcher matcher : matchers) {
            matcher.startSession();
        }

        // --------------

        // Removals first.
        for (InMemPartitionDataProducer source : sources) {
            String alias = source.getTargetTableFQN().getAlias();
            Matcher[] matcherArr = sourceTblAndMatchers.get(alias);
            Map<Long, Object> localRowIdAndCorrIds = sourceTblRowIdAndCorrIds.get(alias);

            AppendOnlyPrimitiveLongList removals = source.retrieveDeadRowIdsInBatch();
            if (removals != null) {
                ArrayList<Object> expelledIds = new ArrayList<Object>(removals.getSize());
                aliasAndExpelledCorrIds.put(alias, expelledIds);

                long[] ids = removals.removeAvailable();
                while (ids.length > 0) {
                    for (int i = 0; i < ids.length; i++) {
                        // Remove from map.
                        Object corrId = localRowIdAndCorrIds.remove(ids[i]);
                        expelledIds.add(corrId);
                    }

                    ids = removals.removeAvailable();
                }

                for (Object corrId : expelledIds) {
                    for (int m = 0; m < matcherArr.length; m++) {
                        matcherArr[m].eventExpelled(alias, corrId);
                    }
                }
            }
        }

        for (Matcher matcher : matchers) {
            List corrIds = matcher.endExpulsions();
            handleExpulsionsAndArrivals(matcher, corrIds, intermediateResults);
        }

        // --------------

        // Additions next.
        for (InMemPartitionDataProducer source : sources) {
            String alias = source.getTargetTableFQN().getAlias();
            Matcher[] matcherArr = sourceTblAndMatchers.get(alias);
            Map<Long, Object> localRowIdAndCorrIds = sourceTblRowIdAndCorrIds.get(alias);
            Map<Object, Object[]> localCorrIdAndData = sourceTblCorrIdAndData.get(alias);

            List<Row> rows = source.retrieveNewRowsInBatch();
            if (rows != null) {
                Row[] additions = rows.toArray(new Row[rows.size()]);
                int rowIdPosition = correlationSpec.getSourceTblAndRowIdPosition().get(alias);
                int corrIdPosition = correlationSpec.getSourceTblAndCorrIdPosition().get(alias);

                for (Row row : additions) {
                    Object[] data = row.getColumns();
                    Number rowId = (Number) data[rowIdPosition];
                    Object corrId = data[corrIdPosition];

                    localRowIdAndCorrIds.put(rowId.longValue(), corrId);
                    localCorrIdAndData.put(corrId, data);
                }

                for (Row row : additions) {
                    for (Matcher matcher : matcherArr) {
                        Object[] data = row.getColumns();
                        Object corrId = data[corrIdPosition];

                        matcher.eventArrived(alias, corrId);
                    }
                }
            }
        }

        for (Matcher matcher : matchers) {
            List corrIds = matcher.endArrivals();
            handleExpulsionsAndArrivals(matcher, corrIds, intermediateResults);
        }

        // --------------

        for (Matcher matcher : matchers) {
            matcher.endSession();
        }

        for (String alias : aliasAndExpelledCorrIds.keySet()) {
            Map<Object, Object[]> localCorrIdAndData = sourceTblCorrIdAndData.get(alias);

            ArrayList<Object> ids = aliasAndExpelledCorrIds.get(alias);
            for (Object corrId : ids) {
                localCorrIdAndData.remove(corrId);
            }
        }

        // --------------

        return Util.filterRows(correlationSpec.getStatement(), rowFilter, columnEvaluators,
                intermediateResults);
    }

    protected void handleExpulsionsAndArrivals(Matcher matcher, List corrIds, List<Object[]> results) {
        String[] aliases = matcher.getPresentAliases();

        ArrayList<Integer[][]> aliasAndColumnMaps = new ArrayList<Integer[][]>(aliases.length);
        ArrayList<Map<Object, Object[]>> localSourceTblAndData = new ArrayList<Map<Object, Object[]>>();

        for (int i = 0; i < aliases.length; i++) {
            Map<String, Integer[][]> allPositions = correlationSpec.getSourceTblAndDestPosition();
            Integer[][] map = allPositions.get(aliases[i]);
            aliasAndColumnMaps.add(map);

            Map<Object, Object[]> localSrcTblAndData = sourceTblCorrIdAndData.get(aliases[i]);
            localSourceTblAndData.add(localSrcTblAndData);
        }

        // Clear the stored Data.
        if (corrIds != null) {
            for (Object corrId : corrIds) {
                Object[] result = new Object[totalResultTableColumns];

                for (int i = 0; i < aliases.length; i++) {
                    Map<Object, Object[]> storedData = localSourceTblAndData.get(i);
                    Integer[][] map = aliasAndColumnMaps.get(i);

                    Object[] data = storedData.get(corrId);
                    if (data != null) {
                        for (int j = 0; j < map.length; j++) {
                            result[map[j][1]] = data[map[j][0]];
                        }
                    }
                }

                results.add(result);
            }
        }
    }
}
