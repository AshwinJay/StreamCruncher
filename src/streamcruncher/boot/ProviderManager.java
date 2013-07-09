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
package streamcruncher.boot;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import streamcruncher.api.Provider;
import streamcruncher.api.TimeWindowSizeProvider;
import streamcruncher.api.WindowSizeProvider;
import streamcruncher.api.aggregator.DiffBaselineProvider;
import streamcruncher.util.LoggerManager;

/*
 * Author: Ashwin Jayaprakash Date: Sep 23, 2006 Time: 1:37:19 PM
 */

public class ProviderManager implements Component {
    protected final ConcurrentMap<String, Class<? extends Provider>> providers;

    public ProviderManager() {
        providers = new ConcurrentHashMap<String, Class<? extends Provider>>();
    }

    /**
     * @param name
     *            A simple convention must be followed. Ex:
     *            {@link DiffBaselineProvider} for the <code>$diff</code>
     *            directive in Aggregates must use
     *            "BaselineProvider/HighestValInLast5Mins" or
     *            "BaselineProvider/Stocks/BiggestLoser" etc. Slash separated
     *            hierarchical names to distinguish between different Providers
     *            registered in the Kernel.
     * @param providerClass
     * @throws ProviderManagerException
     */
    public void registerProvider(String name, Class<? extends Provider> providerClass)
            throws ProviderManagerException {
        if (providers.containsKey(name)) {
            throw new ProviderManagerException("The Provider: " + name + " is already in use.");
        }

        providers.put(name, providerClass);
    }

    public Collection<String> getAllProviderNames() {
        return providers.keySet();
    }

    public boolean doesProviderExist(String name) {
        Class<? extends Provider> providerClass = providers.get(name);
        return providerClass != null;
    }

    public Provider createProvider(String name) throws ProviderManagerException {
        Class<? extends Provider> providerClass = providers.get(name);
        if (providerClass == null) {
            return null;
        }

        // ----------------

        Provider provider = null;
        try {
            provider = providerClass.newInstance();
        }
        catch (Exception e) {
            throw new ProviderManagerException(e);
        }
        return provider;
    }

    public void unregisterProvider(String name) {
        providers.remove(name);
    }

    // ---------------------

    public void start(Object... params) throws Exception {
        registerProvider(WindowSizeProvider.getName(), WindowSizeProvider.class);
        registerProvider(TimeWindowSizeProvider.getName(), TimeWindowSizeProvider.class);

        registerProvider(DiffBaselineProvider.getName(), DiffBaselineProvider.class);

        // --------------------

        Logger logger = Registry.getImplFor(LoggerManager.class).getLogger(
                ProviderManager.class.getName());
        logger.log(Level.INFO, "Started");
    }

    public void stop() throws Exception {
        providers.clear();

        // --------------------

        Logger logger = Registry.getImplFor(LoggerManager.class).getLogger(
                ProviderManager.class.getName());
        logger.log(Level.INFO, "Stopped");
    }
}
