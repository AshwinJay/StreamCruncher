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
package streamcruncher.kernel;

import streamcruncher.boot.Registry;
import streamcruncher.innards.core.filter.FilteredTable;
import streamcruncher.util.TimeKeeper;

/*
 * Author: Ashwin Jayaprakash Date: Sep 9, 2006 Time: 4:30:15 PM
 */

/**
 * Inherits from main package to prevent any leak of other packages into
 * outer/exposed API.
 */
public class QueryConfig extends streamcruncher.api.QueryConfig {
    private static final long serialVersionUID = 1L;

    protected transient final TimeKeeper timeKeeper;

    protected final QueryConfigChangeListener changeListener;

    protected QueryConfig(FilteredTable[] filteredTables, QueryConfigChangeListener changeListener) {
        super(filteredTables);

        this.timeKeeper = Registry.getImplFor(TimeKeeper.class);
        this.changeListener = changeListener;
    }

    protected void copyConfig(QueryConfig config) {
        this.setQuerySchedulePolicy(config.getQuerySchedulePolicy());

        for (String key : config.getUnprocessedEventWeightKeys()) {
            this.setUnprocessedEventWeight(key, config.getUnprocessedEventWeight(key));
        }

        for (String key : config.getAllowedPendingEventsKeys()) {
            this.setAllowedPendingEvents(key, config.getAllowedPendingEvents(key));
        }

        if (config.isQueryPaused()) {
            this.pauseQuery();
        }

        this.setForceScheduleMarginMsecs(config.getForceScheduleMarginMsecs());

        this.setResumeCheckTimeMsecs(config.getResumeCheckTimeMsecs());

        this.setStuckJobInterruptionTimeMsecs(config.getStuckJobInterruptionTimeMsecs());
    }

    // ------------

    /**
     * Also, resets
     * {@link streamcruncher.api.QueryConfig#getQueryRunCount() run-count}, and
     * sets
     * {@link streamcruncher.api.QueryConfig#getQueryLastRanAt() las-ran-at}.
     */
    protected void queryRunSucceeded() {
        queryRunCount++;
        queryErrorCount = queryErrorCount / 2;
        queryLastRanAt = timeKeeper.getTimeMsecs();
    }

    protected void incrementQueryErrorCount() {
        queryErrorCount++;
    }

    // --------------

    @Override
    public void setQuerySchedulePolicy(QuerySchedulePolicyValue policyValue) {
        QuerySchedulePolicyValue oldValue = getQuerySchedulePolicy();

        super.setQuerySchedulePolicy(policyValue);

        QuerySchedulePolicyValue newValue = getQuerySchedulePolicy();

        // Set was allowed.
        if (newValue == policyValue) {
            changeListener.querySchedulePolicyChanged(oldValue, newValue);
        }
    }
}
