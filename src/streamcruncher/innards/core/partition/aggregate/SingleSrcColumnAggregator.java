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

import streamcruncher.api.aggregator.AbstractAggregator;
import streamcruncher.api.aggregator.DiffBaselineProvider;
import streamcruncher.innards.query.Aggregator;
import streamcruncher.innards.query.Aggregator.Extra;

/*
 * Author: Ashwin Jayaprakash Date: Sep 26, 2006 Time: 9:07:15 PM
 */

public abstract class SingleSrcColumnAggregator extends AbstractAggregator {
    private int columnPosition;

    private Aggregator.Extra extra;

    private String extraDiffBaselineProviderName;

    @Override
    public void init(String[] params, LinkedHashMap<String, String> columnNamesAndTypes,
            AggregationStage aggregationStage) {
        super.init(params, columnNamesAndTypes, aggregationStage);

        for (String column : columnNamesAndTypes.keySet()) {
            if (params[0].equalsIgnoreCase(column)) {
                break;
            }

            columnPosition++;
        }

        extra = Extra.NONE;
        extraDiffBaselineProviderName = null;
        // Extras. Currently, only one at a time is supported.
        int c = 1;
        if (params.length > 1) {
            for (Extra e : Extra.values()) {
                if (params[c].equalsIgnoreCase(e.name())) {
                    extra = e;
                    c++;

                    if (params.length > 2) {
                        extraDiffBaselineProviderName = params[c];
                        extraDiffBaselineProviderName = extraDiffBaselineProviderName.replaceAll(
                                "\'", "");
                        c++;
                    }
                    else {
                        extraDiffBaselineProviderName = DiffBaselineProvider.getName();
                    }

                    break;
                }
            }
        }
    }

    public int getColumnPosition() {
        return columnPosition;
    }

    public Aggregator.Extra getExtra() {
        return extra;
    }

    protected String getExtraDiffBaselineProviderName() {
        return extraDiffBaselineProviderName;
    }
}
