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
package streamcruncher.innards.core.partition.aggregate;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import streamcruncher.api.aggregator.AbstractAggregatorHelper;
import streamcruncher.boot.Component;
import streamcruncher.boot.Registry;
import streamcruncher.innards.core.partition.aggregate.impl.AvgAggregatorHelper;
import streamcruncher.innards.core.partition.aggregate.impl.CountAggregatorHelper;
import streamcruncher.innards.core.partition.aggregate.impl.GeoMeanAggregatorHelper;
import streamcruncher.innards.core.partition.aggregate.impl.KurtosisAggregatorHelper;
import streamcruncher.innards.core.partition.aggregate.impl.MaxAggregatorHelper;
import streamcruncher.innards.core.partition.aggregate.impl.MedianAggregatorHelper;
import streamcruncher.innards.core.partition.aggregate.impl.MinAggregatorHelper;
import streamcruncher.innards.core.partition.aggregate.impl.SkewnessAggregatorHelper;
import streamcruncher.innards.core.partition.aggregate.impl.StdDevAggregatorHelper;
import streamcruncher.innards.core.partition.aggregate.impl.SumAggregatorHelper;
import streamcruncher.innards.core.partition.aggregate.impl.SumSqAggregatorHelper;
import streamcruncher.innards.core.partition.aggregate.impl.VarianceAggregatorHelper;
import streamcruncher.util.LoggerManager;

/*
 * Author: Ashwin Jayaprakash Date: Sep 23, 2006 Time: 1:37:19 PM
 */

public class AggregatorManager implements Component {
    protected final ConcurrentMap<String, AbstractAggregatorHelper> aggregatorHelpers;

    public AggregatorManager() {
        aggregatorHelpers = new ConcurrentHashMap<String, AbstractAggregatorHelper>();
    }

    /**
     * Registers under the {@link AbstractAggregatorHelper#getFunctionName()}.
     * 
     * @param helper
     * @throws AggregatorManagerException
     */
    public void registerHelper(AbstractAggregatorHelper helper) throws AggregatorManagerException {
        String fnName = helper.getFunctionName();

        if (aggregatorHelpers.containsKey(fnName)) {
            throw new AggregatorManagerException("The Aggregator: " + fnName
                    + " is already in use.");
        }

        aggregatorHelpers.put(fnName, helper);
    }

    public Collection<AbstractAggregatorHelper> getAllHelpers() {
        return aggregatorHelpers.values();
    }

    public AbstractAggregatorHelper getHelper(String functionName) {
        return aggregatorHelpers.get(functionName);
    }

    public void unregisterHelper(String functionName) {
        aggregatorHelpers.remove(functionName);
    }

    // ---------------------

    public void start(Object... params) throws Exception {
        registerHelper(new AvgAggregatorHelper());
        registerHelper(new CountAggregatorHelper());
        registerHelper(new GeoMeanAggregatorHelper());
        registerHelper(new KurtosisAggregatorHelper());
        registerHelper(new MaxAggregatorHelper());
        registerHelper(new MedianAggregatorHelper());
        registerHelper(new MinAggregatorHelper());
        registerHelper(new SkewnessAggregatorHelper());
        registerHelper(new StdDevAggregatorHelper());
        registerHelper(new SumAggregatorHelper());
        registerHelper(new SumSqAggregatorHelper());
        registerHelper(new VarianceAggregatorHelper());

        // --------------------

        Logger logger = Registry.getImplFor(LoggerManager.class).getLogger(
                AggregatorManager.class.getName());
        logger.log(Level.INFO, "Started");
    }

    public void stop() throws Exception {
        aggregatorHelpers.clear();

        // --------------------

        Logger logger = Registry.getImplFor(LoggerManager.class).getLogger(
                AggregatorManager.class.getName());
        logger.log(Level.INFO, "Stopped");
    }
}
