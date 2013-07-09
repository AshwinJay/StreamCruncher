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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import streamcruncher.boot.Component;
import streamcruncher.boot.Registry;
import streamcruncher.innards.core.partition.ChainedPartitioner;
import streamcruncher.innards.core.partition.EncapsulatedPartitioner;
import streamcruncher.innards.core.partition.StreamReaderMemWriterPartitioner;
import streamcruncher.innards.core.partition.StreamReaderTableWriterPartitioner;
import streamcruncher.innards.core.partition.inmem.InMemChainedPartitioner;
import streamcruncher.innards.core.partition.inmem.InMemPartitioner;
import streamcruncher.util.LoggerManager;

/*
 * Author: Ashwin Jayaprakash Date: Feb 1, 2006 Time: 11:41:28 PM
 */

public class TableFilterClassLoaderManager implements Component {
    protected final ConcurrentMap<String, ClassLoader> filterClassAndLoaderMap;

    public TableFilterClassLoaderManager() {
        filterClassAndLoaderMap = new ConcurrentHashMap<String, ClassLoader>();
    }

    public void registerClassLoader(String filterClassFQN, ClassLoader classLoader) {
        filterClassAndLoaderMap.put(filterClassFQN, classLoader);
    }

    public ClassLoader getClassLoader(String filterClassFQN) {
        return filterClassAndLoaderMap.get(filterClassFQN);
    }

    public void unregisterClassLoader(String filterClassFQN) {
        filterClassAndLoaderMap.remove(filterClassFQN);
    }

    // ---------------------

    public void start(Object... params) throws Exception {
        filterClassAndLoaderMap.put(StreamReaderTableWriterPartitioner.class.getName(),
                StreamReaderTableWriterPartitioner.class.getClassLoader());
        filterClassAndLoaderMap.put(StreamReaderMemWriterPartitioner.class.getName(),
                StreamReaderMemWriterPartitioner.class.getClassLoader());
        filterClassAndLoaderMap.put(InMemPartitioner.class.getName(), InMemPartitioner.class
                .getClassLoader());

        filterClassAndLoaderMap.put(EncapsulatedPartitioner.class.getName(),
                EncapsulatedPartitioner.class.getClassLoader());

        filterClassAndLoaderMap.put(ChainedPartitioner.class.getName(), ChainedPartitioner.class
                .getClassLoader());
        filterClassAndLoaderMap.put(InMemChainedPartitioner.class.getName(),
                InMemChainedPartitioner.class.getClassLoader());

        // --------------------

        Logger logger = Registry.getImplFor(LoggerManager.class).getLogger(
                TableFilterClassLoaderManager.class.getName());
        logger.log(Level.INFO, "Started");
    }

    public void stop() throws Exception {
        filterClassAndLoaderMap.clear();

        // --------------------

        Logger logger = Registry.getImplFor(LoggerManager.class).getLogger(
                TableFilterClassLoaderManager.class.getName());
        logger.log(Level.INFO, "Stopped");
    }
}
