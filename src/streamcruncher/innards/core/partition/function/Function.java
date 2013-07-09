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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.PriorityQueue;
import java.util.Set;

import streamcruncher.api.artifact.RowSpec;
import streamcruncher.innards.core.QueryContext;
import streamcruncher.innards.core.partition.CalculateTSFunctionPair;
import streamcruncher.innards.core.partition.RowBuffer;
import streamcruncher.util.AppendOnlyPrimitiveLongList;
import streamcruncher.util.AtomicX;
import streamcruncher.util.PerpetualResultSet;

/*
 * Author: Ashwin Jayaprakash Date: Feb 13, 2006 Time: 11:37:11 PM
 */

public abstract class Function {
    /**
     * Defaults to <code>this</code>.
     */
    protected Function homeFunction;

    /**
     * Defaults to <code>false</code>.
     */
    protected boolean pinned;

    protected Set<Function> dirtyFunctions;

    protected Set<Function> unprocessedDataFunctions;

    protected PriorityQueue<CalculateTSFunctionPair> calculateTSFunctionPairs;

    // -----------------

    protected final AtomicX rowIdGenerator;

    protected final AppendOnlyPrimitiveLongList oustedRowIds;

    protected final RowSpec realTableRowSpec;

    protected final RowSpec finalTableRowSpec;

    protected final RowBuffer unprocessedRowBuffer;

    protected final RowBuffer processedRowBuffer;

    private boolean processInvokedAtleastOnce;

    private boolean cycleStartInvoked;

    private boolean dirtyAdded;

    private boolean newCycle;

    private boolean calculateInvoked;

    private boolean canDiscard;

    private int unprocessedBufferSizeBeforeExec;

    /**
     * @param realTableRowSpec
     * @param finalTableRowSpec
     * @param rowIdGenerator
     *            for the {@link #processedRowBuffer}.
     */
    public Function(RowSpec realTableRowSpec, RowSpec finalTableRowSpec, AtomicX rowIdGenerator) {
        this.oustedRowIds = new AppendOnlyPrimitiveLongList(8);
        this.realTableRowSpec = realTableRowSpec;
        this.finalTableRowSpec = finalTableRowSpec;
        this.unprocessedRowBuffer = new RowBuffer(realTableRowSpec, null);
        this.processedRowBuffer = new RowBuffer(finalTableRowSpec, rowIdGenerator);
        this.rowIdGenerator = rowIdGenerator;

        this.homeFunction = this;
    }

    /**
     * @return Returns the homeFunction.
     */
    public Function getHomeFunction() {
        return homeFunction;
    }

    /**
     * @param homeFunction
     *            The homeFunction to set.
     */
    public void setHomeFunction(Function homeFunction) {
        this.homeFunction = homeFunction;
    }

    /**
     * @return Returns the dirtyFunctions.
     */
    public Set<Function> getDirtyFunctions() {
        return dirtyFunctions;
    }

    /**
     * @param dirtyFunctions
     *            The dirtyFunctions to set.
     */
    public void setDirtyFunctions(Set<Function> dirtyFunctions) {
        this.dirtyFunctions = dirtyFunctions;
    }

    /**
     * @return Returns the unprocessedDataFunctions.
     */
    public Set<Function> getUnprocessedDataFunctions() {
        return unprocessedDataFunctions;
    }

    /**
     * @param unprocessedDataFunctions
     *            The unprocessedDataFunctions to set.
     */
    public void setUnprocessedDataFunctions(Set<Function> unprocessedFunctions) {
        this.unprocessedDataFunctions = unprocessedFunctions;
    }

    /**
     * @return Returns the calculateTSFunctionPairs.
     */
    public PriorityQueue<CalculateTSFunctionPair> getCalculateTSFunctionPairs() {
        return calculateTSFunctionPairs;
    }

    /**
     * @param calculateTSFunctionPairs
     *            The calculateTSFunctionPairs to set.
     */
    public void setCalculateTSFunctionPairs(
            PriorityQueue<CalculateTSFunctionPair> calculateTSFunctionPairs) {
        this.calculateTSFunctionPairs = calculateTSFunctionPairs;
    }

    /**
     * @return Returns the oustedRowIds.
     */
    public AppendOnlyPrimitiveLongList getOustedRowIds() {
        return oustedRowIds;
    }

    /**
     * @return Returns the processedRowBuffer.
     */
    public RowBuffer getProcessedRowBuffer() {
        return processedRowBuffer;
    }

    /**
     * @return the finalTableRowSpec
     */
    public RowSpec getFinalTableRowSpec() {
        return finalTableRowSpec;
    }

    /**
     * @return the realTableRowSpec
     */
    public RowSpec getRealTableRowSpec() {
        return realTableRowSpec;
    }

    /**
     * @return Returns the unprocessedRowBuffer.
     */
    public RowBuffer getUnprocessedRowBuffer() {
        return unprocessedRowBuffer;
    }

    /**
     * @return Returns the processInvokedAtleastOnce.
     */
    protected boolean wasProcessInvokedAtleastOnce() {
        return processInvokedAtleastOnce;
    }

    /**
     * @return Returns the newCycle.
     */
    protected boolean isNewCycle() {
        return newCycle;
    }

    public void setPinned(boolean pinned) {
        this.pinned = pinned;
    }

    public boolean isPinned() {
        return pinned;
    }

    // -----------------

    public final void calculate(QueryContext context) {
        onCalculate(context);
        calculateInvoked = true;
    }

    protected void onCalculate(QueryContext context) {
    }

    // -----------------

    public final void cycleStart(QueryContext context) throws Exception {
        newCycle = true;

        unprocessedBufferSizeBeforeExec = unprocessedRowBuffer.getRows().size();

        // Lazy calculation.
        if (calculateInvoked == false) {
            calculate(context);
        }

        onCycleStart(context);

        cycleStartInvoked = true;

        // Could've been added by the Table-Partitioner.
        if (dirtyFunctions.contains(getHomeFunction())) {
            dirtyAdded = true;
        }
    }

    protected void onCycleStart(QueryContext context) throws Exception {
    }

    @SuppressWarnings("unchecked")
    public final void handleRow(QueryContext context, PerpetualResultSet currRow) throws Exception {
        if (cycleStartInvoked == false) {
            cycleStart(context);
        }

        if (dirtyAdded == false) {
            dirtyFunctions.add(getHomeFunction());
            dirtyAdded = true;
        }

        process(context, currRow);

        if (processInvokedAtleastOnce == false) {
            processInvokedAtleastOnce = true;
        }

        if (newCycle == true) {
            newCycle = false;
        }
    }

    /**
     * @param context
     * @param currRow
     *            This Row <b>has</b> to be consumed - Stored in a buffer or
     *            added to the final Table etc.
     * @throws Exception
     */
    protected abstract void process(QueryContext context, PerpetualResultSet currRow)
            throws Exception;

    /**
     * @param context
     * @return <code>true</code> if this function can be discarded - usually,
     *         when there are no Rows in this leaf-node.
     * @throws SQLException
     */
    @SuppressWarnings("unchecked")
    public final boolean cycleEnd(QueryContext context) throws Exception {
        dirtyAdded = true;

        canDiscard = onCycleEnd(context);
        canDiscard = pinned ? false /* Never discard if pinned. */: canDiscard;

        processInvokedAtleastOnce = false;
        calculateInvoked = false;
        newCycle = true;
        cycleStartInvoked = false;
        dirtyAdded = false;

        int totalBeforeExec = context.getTotalUnprocessedBufferedRows();
        int afterExec = unprocessedRowBuffer.getRows().size();

        if (afterExec > 0 && canDiscard == false) {
            unprocessedDataFunctions.add(getHomeFunction());
        }

        int newTotal = totalBeforeExec - unprocessedBufferSizeBeforeExec + afterExec;
        context.setTotalUnprocessedBufferedRows(newTotal);

        return canDiscard;
    }

    /**
     * This method is called after {@link #process(ResultSet)} is invoked. After
     * this method is invoked, {@link #getOustedRowIds()} and
     * {@link #getProcessedRowBuffer()} is invoked. So, this method should set
     * up those fields to match this simCycle's results.
     * 
     * @param context
     * @return <code>true</code> if this function can be discarded - usually,
     *         when there are no Rows in this leaf-node.
     * @throws SQLException
     */
    protected abstract boolean onCycleEnd(QueryContext context) throws Exception;

    /**
     * @return The value returned by {@link #cycleEnd()}.
     */
    public boolean canDiscard() {
        return canDiscard;
    }
}
