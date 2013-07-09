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
package streamcruncher.innards.core.partition.function;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.TreeMap;

import streamcruncher.api.artifact.RowSpec;
import streamcruncher.innards.core.QueryContext;
import streamcruncher.innards.core.partition.Row;
import streamcruncher.util.AtomicX;
import streamcruncher.util.PerpetualResultSet;

/*
 * Author: Ashwin Jayaprakash Date: Feb 19, 2006 Time: 4:30:16 PM
 */

public class SortedWindowFunction extends WindowFunction {
    protected final int windowSize;

    protected final int columnPosition;

    protected final Type type;

    protected final TreeMap<Comparable, LinkedHashSet<Long>> sortedData;

    protected final HashMap<Long, GroupHashComparablePair> itemsInWindow;

    protected final HashMap</* Group Hash */Long, IdComparablePair> columnGroupHashesInWindow;

    protected final int[] sameGroupColumnsPositions;

    /**
     * @param pinned
     * @param selectedRowSpec
     * @param newRowSpec
     * @param rowIdGenerator
     * @param sourceLocationForTargetCols
     * @param windowSize
     * @param columnName
     * @param sameGroupColumns
     *            If empty array is sent, then there is no check on repeating
     *            groups.
     * @param type
     */
    public SortedWindowFunction(RowSpec selectedRowSpec, RowSpec newRowSpec,
            AtomicX rowIdGenerator, int[] sourceLocationForTargetCols, int windowSize,
            String columnName, String[] sameGroupColumns, Type type) {
        super(selectedRowSpec, newRowSpec, rowIdGenerator, sourceLocationForTargetCols, windowSize);

        this.windowSize = windowSize;

        this.columnPosition = findColumnPosition(selectedRowSpec, columnName);

        this.type = type;
        this.sortedData = new TreeMap<Comparable, LinkedHashSet<Long>>();
        this.itemsInWindow = new HashMap<Long, GroupHashComparablePair>();

        this.columnGroupHashesInWindow = new HashMap<Long, IdComparablePair>();
        this.sameGroupColumnsPositions = new int[sameGroupColumns.length];
        for (int i = 0; i < sameGroupColumns.length; i++) {
            sameGroupColumnsPositions[i] = findColumnPosition(selectedRowSpec, sameGroupColumns[i]);
        }
    }

    private int findColumnPosition(RowSpec selectedRowSpec, String columnName) {
        int pos = 0;
        for (String column : selectedRowSpec.getColumnNames()) {
            if (columnName.equalsIgnoreCase(column)) {
                break;
            }

            pos++;
        }

        return pos;
    }

    /**
     * @return Returns the windowSize.
     */
    public int getWindowSize() {
        return windowSize;
    }

    // -------------------

    @Override
    /**
     * @param context
     */
    public void onCalculate(QueryContext context) {
        free = 0;
        maxRowsThatCanBeConsumed = windowSize;
    }

    @Override
    /**
     * return <code>true</code>. Allow freeing even if there are no Rows to
     * be consumed.
     */
    protected boolean allowFreeingWhenRSIsNull() {
        return false;
    }

    @Override
    protected void process(QueryContext context, PerpetualResultSet currRow) throws Exception {
        if (currRow == null) {
            return;
        }

        Row toBuffer = unprocessedRowBuffer.addNewRow();
        String[] unprocColumnNames = realTableRowSpec.getColumnNames();
        Object[] columns = toBuffer.getColumns();

        for (int i = 0; i < unprocColumnNames.length; i++) {
            columns[i] = currRow.getColumnValue(unprocColumnNames[i]);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean onCycleEnd(QueryContext context) throws Exception {
        boolean val = super.onCycleEnd(context);

        // -----------------

        HashMap</* Id */Long, /* Hash */Long> cachedGroupHashes = new HashMap<Long, Long>();

        IdentityHashMap<Comparable, Object[]> newValues = new IdentityHashMap<Comparable, Object[]>();

        List<Row> rows = unprocessedRowBuffer.getRows();
        for (Row row : rows) {
            Object[] columns = row.getColumns();
            Object obj = columns[columnPosition];

            if (obj != null && obj instanceof Comparable) {
                Comparable currentVal = (Comparable) obj;
                Long id = ((Number) columns[idColumnBufferPos]).longValue();

                // Group update columns provided.
                if (sameGroupColumnsPositions.length > 0) {
                    long groupHash = 0;
                    for (int i = 0; i < sameGroupColumnsPositions.length; i++) {
                        Object grpMember = columns[sameGroupColumnsPositions[i]];

                        int hash = (grpMember != null) ? grpMember.hashCode() : 0;
                        groupHash = (37 * groupHash) + hash;
                    }

                    cachedGroupHashes.put(id, groupHash);

                    // --------------

                    IdComparablePair oldPair = columnGroupHashesInWindow.get(groupHash);
                    /*
                     * Old value is now being updated. So, old one must be
                     * flushed.
                     */
                    if (oldPair != null) {
                        oustedRowIds.add(oldPair.getId());

                        LinkedHashSet<Long> innerLevel = sortedData.get(oldPair.getComparable());
                        innerLevel.remove(oldPair.getId());
                        if (innerLevel.size() == 0) {
                            sortedData.remove(oldPair.getComparable());
                        }

                        itemsInWindow.remove(oldPair.getId());

                        columnGroupHashesInWindow.remove(groupHash);
                    }
                }

                // --------------

                // Place the new values.
                LinkedHashSet<Long> innerLevel = sortedData.get(currentVal);
                if (innerLevel == null) {
                    innerLevel = new LinkedHashSet<Long>();
                    sortedData.put(currentVal, innerLevel);
                }
                innerLevel.add(id);

                itemsInWindow.put(id, null);

                newValues.put(currentVal, columns);
            }
        }

        rows.clear();

        // -----------------

        // Size maintenance.
        while (itemsInWindow.size() > windowSize) {
            Comparable c = null;

            if (type == Type.HIGHEST) {
                c = sortedData.firstKey();
            }
            else {
                c = sortedData.lastKey();
            }

            /*
             * Start clearing from the earliest entry of the smallest/highest
             * set.
             */
            LinkedHashSet<Long> innerLevel = sortedData.get(c);
            for (Iterator<Long> iter = innerLevel.iterator(); (itemsInWindow.size() > windowSize)
                    && iter.hasNext();) {
                Long expelledId = iter.next();

                iter.remove();

                GroupHashComparablePair hashAndCPair = itemsInWindow.remove(expelledId);
                if (hashAndCPair != null) {
                    // Id made it into the top/bottom x in some previous cycle.
                    oustedRowIds.add(expelledId);
                    columnGroupHashesInWindow.remove(hashAndCPair.getGroupHash());
                }
            }
            if (innerLevel.size() == 0) {
                sortedData.remove(c);
            }
        }

        // -----------------

        if (oustedRowIds.getSize() > 0) {
            // Clear old values.
            int size = getNumOfIds();
            discardFirstXIds(size);
        }

        for (Comparable c : newValues.keySet()) {
            Object[] originalColumns = newValues.get(c);
            Long id = ((Number) originalColumns[idColumnBufferPos]).longValue();

            // New value that made it into the top/bottom x.
            if (itemsInWindow.containsKey(id)) {
                Row newRow = processedRowBuffer.addNewRowWithAutoValues(context.getRunCount());
                Object[] columns = newRow.getColumns();

                for (int i = 0; i < columns.length; i++) {
                    int position = sourceLocationForTargetCols[i];
                    if (position >= 0) {
                        columns[i] = originalColumns[position];
                    }
                }

                addId(id);

                Long hash = cachedGroupHashes.get(id);
                if (hash != null) {
                    columnGroupHashesInWindow.put(hash, new IdComparablePair(c, id));
                }

                /*
                 * Put the Id against the key. So that it can be used to track
                 * removed values.
                 */
                itemsInWindow.put(id, new GroupHashComparablePair(c, hash));
            }
        }

        return val;
    }

    // -----------------

    protected static class IdComparablePair {
        protected final Comparable comparable;

        protected final Long id;

        public IdComparablePair(Comparable comparable, Long id) {
            this.comparable = comparable;
            this.id = id;
        }

        public Comparable getComparable() {
            return comparable;
        }

        public Long getId() {
            return id;
        }
    }

    protected static class GroupHashComparablePair {
        protected final Comparable comparable;

        protected final Long groupHash;

        public GroupHashComparablePair(Comparable comparable, Long groupHash) {
            this.comparable = comparable;
            this.groupHash = groupHash;
        }

        public Comparable getComparable() {
            return comparable;
        }

        public Long getGroupHash() {
            return groupHash;
        }
    }

    public static enum Type {
        HIGHEST, LOWEST;
    }
}