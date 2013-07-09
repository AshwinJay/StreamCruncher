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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import streamcruncher.api.Provider;
import streamcruncher.api.aggregator.DiffBaselineProvider;
import streamcruncher.boot.ProviderManager;
import streamcruncher.boot.Registry;
import streamcruncher.innards.query.Aggregator.Extra;

/*
 * Author: Ashwin Jayaprakash Date: Oct 9, 2006 Time: 10:12:21 PM
 */

/**
 * Attempts to aggregate only non-<code>null</code> {@link Comparable} items.
 */
public abstract class GenericSingleSrcColumnAggregator<T extends Comparable> extends
        SingleSrcColumnAggregator {
    protected TreeMap<T, AtomicInteger> valuesAndCounts;

    protected int totalCount;

    private T cachedValue;

    private T extraOldValue;

    protected DiffBaselineProvider<T> extraDiffBaselineProvider;

    @Override
    public void init(String[] params, LinkedHashMap<String, String> columnNamesAndTypes,
            AggregationStage aggregationStage) {
        super.init(params, columnNamesAndTypes, aggregationStage);

        valuesAndCounts = new TreeMap<T, AtomicInteger>();

        String extraDiffBaselineProviderName = getExtraDiffBaselineProviderName();
        if (extraDiffBaselineProviderName != null) {
            try {
                ProviderManager providerManager = Registry.getImplFor(ProviderManager.class);
                Provider provider = providerManager.createProvider(extraDiffBaselineProviderName);

                extraDiffBaselineProvider = (DiffBaselineProvider<T>) provider;
            }
            catch (Exception e) {
                String msg = "Could not instantiate the customized "
                        + DiffBaselineProvider.class.getSimpleName() + "<Comparable>: "
                        + extraDiffBaselineProviderName;

                throw new RuntimeException(msg, e);
            }
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @return <code>null</code> when the Aggregate list is empty.
     */
    @SuppressWarnings("unchecked")
    @Override
    public T aggregate(List<Object[]> removedValues, List<Object[]> addedValues) {
        final int pos = getColumnPosition();

        boolean valueChanged = false;

        if (removedValues != null && getAggregationStage() != AggregationStage.ENTRANCE) {
            for (Object[] objects : removedValues) {
                Object object = objects[pos];

                // Consider only non-nulls.
                if (object != null && object instanceof Comparable) {
                    Comparable c = (Comparable) object;

                    AtomicInteger count = valuesAndCounts.get(c);
                    if (count.decrementAndGet() == 0) {
                        valuesAndCounts.remove(c);
                    }

                    totalCount--;
                    valueChanged = true;
                }
            }
        }

        if (addedValues != null) {
            for (Object[] objects : addedValues) {
                Object object = objects[pos];

                // Consider only non-nulls.
                if (object != null && object instanceof Comparable) {
                    T c = (T) object;

                    AtomicInteger count = valuesAndCounts.get(c);
                    if (count == null) {
                        count = new AtomicInteger(0);
                        valuesAndCounts.put(c, count);
                    }

                    count.incrementAndGet();
                    totalCount++;
                    valueChanged = true;
                }
            }
        }

        if (valuesAndCounts.size() > 0) {
            if (valueChanged) {
                T newValue = fetchAggregatedValue();
                cachedValue = doExtra(newValue);
            }
        }
        else {
            cachedValue = doExtra(null);
        }

        return cachedValue;
    }

    /**
     * This method gets called only if the {@link #valuesAndCounts} has <b>at
     * least 1 element</b>.
     * 
     * @return some value stored in {@link #valuesAndCounts}.
     */
    protected abstract T fetchAggregatedValue();

    protected T doExtra(T newValue) {
        if (getExtra() == Extra.DIFF) {
            T baseline = extraDiffBaselineProvider.getBaseline(extraOldValue, newValue);
            T diff = doDiff(baseline, newValue);

            extraOldValue = newValue;

            return diff;
        }

        return newValue;
    }

    /**
     * Return NewValue - OldValue.
     * 
     * @param oldValue
     * @param newValue
     * @return
     */
    protected abstract T doDiff(T oldValue, T newValue);
}
