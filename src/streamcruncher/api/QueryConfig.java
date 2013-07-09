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
package streamcruncher.api;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import streamcruncher.innards.core.filter.FilteredTable;
import streamcruncher.util.FixedKeyHashMap;

/*
 * Author: Ashwin Jayaprakash Date: Jul 3, 2006 Time: 10:04:57 PM
 */

/**
 * <p>
 * Each registered Query has a configuration object that can be retrieved after
 * parsing or at any time during the life of the Query from the API provided.
 * </p>
 * <p>
 * A handle to the instance must not be serialized and stored through a restart,
 * but must be retrieved afresh after the Kernel restarts. Any changes made to
 * the configuration will take effect immediately. This class <b>is Thread-safe</b>.
 * All values have defaults.
 * </p>
 */
public abstract class QueryConfig implements Serializable {
    public static final QuerySchedulePolicy defaultSchPolicy = QuerySchedulePolicy.ATLEAST_OR_SOONER;

    /**
     * {@value}
     */
    public static final long defaultSchTimeMillis = 1500;

    /**
     * {@value}
     */
    public static final float defaultEventWeight = 1.0f;

    /**
     * {@value}
     */
    public static final int defaultAllowedPendingEvents = Integer.MAX_VALUE;

    /**
     * {@value}
     */
    public static final long defaultForceScheduleMarginMsecs = 250;

    /**
     * {@value}
     */
    public static final long defaultResumeCheckTimeMsecs = 5000;

    /**
     * {@value}
     */
    public static final long defaultStuckJobIntTimeMsecs = 45 * 1000;

    // ------------

    protected final FilteredTable[] filteredTables;

    // ------------

    protected volatile QuerySchedulePolicyValue querySchedulePolicy;

    protected final Set<String> keys;

    protected final FixedKeyHashMap<String, Float> unprocessedEventWeights;

    protected final FixedKeyHashMap<String, Integer> allowedPendingEvents;

    // ------------

    protected transient volatile long queryLastRanAt;

    protected transient volatile int queryRunCount;

    protected transient volatile int queryErrorCount;

    // ------------

    protected volatile boolean paused;

    protected volatile long forceScheduleMarginMsecs;

    protected volatile long resumeCheckTimeMsecs;

    protected volatile long stuckJobInterruptionTimeMsecs;

    // ------------

    protected QueryConfig(FilteredTable[] filteredTables) {
        this.filteredTables = filteredTables;

        HashSet<String> fqns = new HashSet<String>();
        for (FilteredTable table : filteredTables) {
            String fqn = table.getSourceTableFQN().getFQN();
            fqns.add(fqn);
        }
        this.keys = Collections.unmodifiableSet(fqns);

        querySchedulePolicy = new QuerySchedulePolicyValue(defaultSchPolicy, defaultSchTimeMillis);

        unprocessedEventWeights = new FixedKeyHashMap<String, Float>(keys, defaultEventWeight);

        allowedPendingEvents = new FixedKeyHashMap<String, Integer>(keys,
                defaultAllowedPendingEvents);

        resumeCheckTimeMsecs = defaultResumeCheckTimeMsecs;

        stuckJobInterruptionTimeMsecs = defaultStuckJobIntTimeMsecs;
    }

    // ------------

    /**
     * @return Time in Milliseconds at which this Query ran successfully.
     */
    public long getQueryLastRanAt() {
        return queryLastRanAt;
    }

    /**
     * @return Number of <b>consecutive</b> Runs that faced errors. Resets to
     *         zero as soon as a Round succeeds.
     */
    public int getQueryErrorCount() {
        return queryErrorCount;
    }

    /**
     * @return Number of Runs that were successful.
     */
    public int getQueryRunCount() {
        return queryRunCount;
    }

    // ------------

    /**
     * @return Unmodifiable Set of Keys that are used in Event-weights and
     *         Pending-events.
     */
    public Set<String> getKeys() {
        return keys;
    }

    // ------------

    public QuerySchedulePolicyValue getQuerySchedulePolicy() {
        return querySchedulePolicy;
    }

    /**
     * Set the Query's scheduling policy. The change will reflect only in the
     * next cycle, whenever it is due, based on the current policy value.
     * 
     * @param policyValue
     */
    public void setQuerySchedulePolicy(QuerySchedulePolicyValue policyValue) {
        if (policyValue != null) {
            this.querySchedulePolicy = policyValue;
        }
    }

    // ------------

    public float getUnprocessedEventWeight(String key) {
        return unprocessedEventWeights.get(key);
    }

    public Set<String> getUnprocessedEventWeightKeys() {
        return unprocessedEventWeights.getKeys();
    }

    /**
     * @param key
     *            The fully-qualified-name (Ex: "stocks.symbols", "traffic" etc)
     *            of the Input Stream/Table that supplies Events to this Query
     *            system.
     * @param weight
     */
    public void setUnprocessedEventWeight(String key, float weight) {
        unprocessedEventWeights.put(key, weight);
    }

    // ------------

    public int getAllowedPendingEvents(String key) {
        return allowedPendingEvents.get(key);
    }

    public Set<String> getAllowedPendingEventsKeys() {
        return allowedPendingEvents.getKeys();
    }

    /**
     * @param key
     *            The fully-qualified-name (Ex: "stocks.symbols", "traffic" etc)
     *            of the Input-Stream/Table that supplies events to this Query
     *            system.
     * @param events
     */
    public void setAllowedPendingEvents(String key, int events) {
        allowedPendingEvents.put(key, events);
    }

    // ------------

    public boolean isQueryPaused() {
        return paused;
    }

    public void pauseQuery() {
        this.paused = true;
    }

    public void resumeQuery() {
        this.paused = false;
    }

    // ------------

    public long getForceScheduleMarginMsecs() {
        return forceScheduleMarginMsecs;
    }

    /**
     * The duration in milliseconds below which a forced schedule will not
     * occur, even if the event-weights have crossed <code>1.0</code> if the
     * time left before a natural/periodic schedule occurs is less than or equal
     * to this margin.
     * 
     * @param forceScheduleMarginMsecs
     */
    public void setForceScheduleMarginMsecs(long forceScheduleMarginMsecs) {
        this.forceScheduleMarginMsecs = (forceScheduleMarginMsecs <= 0) ? defaultForceScheduleMarginMsecs
                : forceScheduleMarginMsecs;
    }

    // ------------

    public long getResumeCheckTimeMsecs() {
        return resumeCheckTimeMsecs;
    }

    /**
     * The duration in milliseconds after which the Query wakes up to check if
     * it has been resumed. If it has not been resumed, then it goes back to
     * sleep.
     * 
     * @param resumeCheckTimeMsecs
     */
    public void setResumeCheckTimeMsecs(long resumeCheckTimeMsecs) {
        this.resumeCheckTimeMsecs = (resumeCheckTimeMsecs <= 0) ? defaultResumeCheckTimeMsecs
                : resumeCheckTimeMsecs;
    }

    // ------------

    public long getStuckJobInterruptionTimeMsecs() {
        return stuckJobInterruptionTimeMsecs;
    }

    /**
     * Some parts of the Query processing are multi-threaded. If some Threads do
     * not complete, then they will be interrupted. The current cycle of the
     * Query will stop but does not affect subsequent runs.
     * 
     * @param stuckJobInterruptionTimeMsecs
     */
    public void setStuckJobInterruptionTimeMsecs(long stuckJobInterruptionTimeMsecs) {
        this.stuckJobInterruptionTimeMsecs = (stuckJobInterruptionTimeMsecs <= 0) ? defaultStuckJobIntTimeMsecs
                : stuckJobInterruptionTimeMsecs;
    }

    // ------------

    public static enum QuerySchedulePolicy {
        /**
         * Query executes strictly at the specified intervals.
         */
        FIXED,
        /**
         * Query executes at the intervals specified or sooner if the combined
         * weight ({@link QueryConfig#getUnprocessedEventWeight(String)}) of
         * the unprocessed Events go above 0, or if an Event in a Time-based or
         * Latest Rows Window is about to expire.
         */
        ATLEAST_OR_SOONER;
    }

    public static class QuerySchedulePolicyValue implements Serializable {
        private static final long serialVersionUID = 1L;

        protected final QuerySchedulePolicy policy;

        protected final long timeMillis;

        public QuerySchedulePolicyValue(QuerySchedulePolicy policy, long timeMillis) {
            this.policy = (policy == null) ? defaultSchPolicy : policy;
            this.timeMillis = (timeMillis <= 0) ? defaultSchTimeMillis : timeMillis;
        }

        public QuerySchedulePolicy getPolicy() {
            return policy;
        }

        public long getTimeMillis() {
            return timeMillis;
        }
    }
}
