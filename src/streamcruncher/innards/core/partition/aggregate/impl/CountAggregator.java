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
package streamcruncher.innards.core.partition.aggregate.impl;

import java.util.LinkedHashMap;
import java.util.List;

import streamcruncher.api.Provider;
import streamcruncher.api.aggregator.DiffBaselineProvider;
import streamcruncher.boot.ProviderManager;
import streamcruncher.boot.Registry;
import streamcruncher.innards.core.partition.aggregate.SingleSrcColumnAggregator;
import streamcruncher.innards.query.Aggregator.Extra;

/*
 * Author: Ashwin Jayaprakash Date: Sep 26, 2006 Time: 10:07:47 PM
 */

public class CountAggregator extends SingleSrcColumnAggregator {
    protected int count;

    protected int extraOldCount;

    protected DiffBaselineProvider<Integer> extraDiffBaselineProvider;

    /**
     * {@inheritDoc}
     * 
     * @throws RuntimeException
     *             if a {@link DiffBaselineProvider} sub-class was provided and
     *             if it could not be created.
     */
    @Override
    public void init(String[] params, LinkedHashMap<String, String> columnNamesAndTypes,
            AggregationStage aggregationStage) {
        super.init(params, columnNamesAndTypes, aggregationStage);

        String extraDiffBaselineProviderName = getExtraDiffBaselineProviderName();
        if (extraDiffBaselineProviderName != null) {
            try {
                ProviderManager providerManager = Registry.getImplFor(ProviderManager.class);
                Provider provider = providerManager.createProvider(extraDiffBaselineProviderName);

                extraDiffBaselineProvider = (DiffBaselineProvider<Integer>) provider;
            }
            catch (Exception e) {
                String msg = "Could not instantiate the customized "
                        + DiffBaselineProvider.class.getSimpleName() + "<Integer>: "
                        + extraDiffBaselineProviderName;

                throw new RuntimeException(msg, e);
            }
        }
    }

    @Override
    public Integer aggregate(List<Object[]> removedValues, List<Object[]> addedValues) {
        if (removedValues != null && getAggregationStage() != AggregationStage.ENTRANCE) {
            count = count - removedValues.size();
        }

        if (addedValues != null) {
            count = count + addedValues.size();
        }

        int result = doExtra(count);
        return result;
    }

    protected int doExtra(int newCount) {
        if (getExtra() == Extra.DIFF) {
            int baseline = extraDiffBaselineProvider.getBaseline(extraOldCount, newCount);
            int diff = newCount - baseline;

            extraOldCount = newCount;

            return diff;
        }

        return newCount;
    }
}
