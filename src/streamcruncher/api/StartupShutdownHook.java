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

import streamcruncher.api.aggregator.AbstractAggregatorHelper;

/*
 * Author: Ashwin Jayaprakash Date: Dec 10, 2006 Time: 1:23:46 PM
 */
/**
 * A Listener interface to listen to the lifecycle of the Kernel.
 */
public interface StartupShutdownHook {
    /**
     * <p>
     * Invoked by the Kernel, <b>after</b> all the internal Components have
     * started successfully <b>but before</b> the Input Event Streams and
     * Queries that were registered in a previous run of the Kernel (if any),
     * are re-registered by the Kernel.
     * </p>
     * <p>
     * This method would be the correct place to register
     * {@linkplain AbstractAggregatorHelper Aggregates} etc.
     * </p>
     */
    public void afterBootup();

    /**
     * Invoked by the Kernel, <b>before</b> all the internal Components are
     * stopped. This also means before the Queries are stopped.
     */
    public void beforeShutdown();
}
