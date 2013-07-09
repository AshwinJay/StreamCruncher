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
package streamcruncher.innards.core.partition.correlation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/*
 * Author: Ashwin Jayaprakash Date: Feb 26, 2007 Time: 6:39:43 PM
 */

public class DelayedMatcher extends ImmediateMatcher {
    protected final Set<Object> waitAndWatchCorrIdList;

    protected final List<Object> emptyList;

    public DelayedMatcher(String[] presentAliases, String[] notPresentAliases) {
        super(presentAliases, notPresentAliases);

        this.waitAndWatchCorrIdList = new HashSet<Object>();
        this.emptyList = new ArrayList<Object>();
    }

    // ----------

    @Override
    public void eventExpelled(String alias, Object id) {
        int bitPositionForAlias = aliasAndBitPatterns.get(alias);

        Integer pattern = workingPatternSet.get(id);
        if (pattern == null) {
            pattern = stalePatternSet.get(id);
            if (pattern == null) {
                // Can't happen.
            }
            else {
                pattern = pattern & ~bitPositionForAlias;

                if (pattern == 0) {
                    stalePatternSet.remove(id);
                    waitAndWatchCorrIdList.remove(id);
                }
                else {
                    // Put it back. Nothing further to do.
                    stalePatternSet.put(id, pattern);
                }
            }
        }
        else {
            Integer oldPattern = pattern;
            pattern = pattern & ~bitPositionForAlias;

            /*
             * Patterns that change to "Hit" when an Event is expelled are
             * ignored. Because it means that a Present and Not-Present Alias
             * was "on" and now the Not-Present Event got expelled, so the
             * Pattern is being asserted now, which is not what we want. We are
             * only looking for Patterns that were "on" and now because one of
             * the Key Aliases is being expelled, the Pattern is "off". Also, we
             * only want to trigger Patterns that got asserted as virgin
             * Present-Only patterns and not ones that changed to Present-Only
             * Patterns along the way.
             */
            if (oldPattern == targetBitPattern && waitAndWatchCorrIdList.contains(id) == true) {
                /*
                 * Put this Corr-Id into potential hit list. Wait for
                 * endExpulsions() to see if this Pattern makes it to the end.
                 */
                corrIdSessionHitList.add(id);
            }

            if (pattern == 0) {
                workingPatternSet.remove(id);
                waitAndWatchCorrIdList.remove(id);
            }
            else {
                // Put it back.
                workingPatternSet.put(id, pattern);
            }
        }
    }

    @Override
    public List<Object> endExpulsions() {
        ArrayList<Object> results = new ArrayList<Object>(corrIdSessionHitList);

        for (Iterator iter = corrIdSessionHitList.iterator(); iter.hasNext();) {
            Object staleId = iter.next();
            iter.remove();

            Integer pattern = workingPatternSet.remove(staleId);

            stalePatternSet.put(staleId, pattern);
            waitAndWatchCorrIdList.remove(staleId);
        }

        return results;
    }

    @Override
    protected void handleWorkingSetArrival(Object id, int bitPositionForAlias, Integer pattern) {
        Integer updatedPattern = pattern | bitPositionForAlias;

        if (updatedPattern == targetBitPattern) {
            /*
             * Put this Corr-Id into potential hit list. Wait for endArrivals()
             * to see if this Pattern makes it to the end.
             */
            corrIdSessionHitList.add(id);
        }
        else {
            /*
             * Withdraw from hit list (if present) as a Not-Required Event also
             * arrived in this cycle along with a Required Event.
             */
            corrIdSessionHitList.remove(id);

            /*
             * And event was asserted as a virgin Present-Only Pattern (110),
             * and then later the Not-Present alias arrived and went out before
             * or along with a Present alias. [110 -> 111 -> 110 -> 100*] Since
             * this scenario does not leave the Pattern as a virgin pattern, it
             * has to be removed from the list.
             */
            waitAndWatchCorrIdList.remove(id);
        }

        workingPatternSet.put(id, updatedPattern);
    }

    @Override
    public List<Object> endArrivals() {
        /*
         * Don't send these off immediately. Wait and watch.
         */
        waitAndWatchCorrIdList.addAll(corrIdSessionHitList);
        corrIdSessionHitList.clear();

        return emptyList;
    }
}
