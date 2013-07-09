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
import java.util.Iterator;
import java.util.List;

/*
 * Author: Ashwin Jayaprakash Date: Feb 26, 2007 Time: 6:39:43 PM
 */

public class ImmediateMatcher extends Matcher {
    public ImmediateMatcher(String[] presentAliases, String[] notPresentAliases) {
        super(presentAliases, notPresentAliases);
    }

    // ----------

    @Override
    public void startSession() {
        corrIdSessionHitList.clear();
    }

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
                }
                else {
                    // Put it back. Nothing further to do.
                    stalePatternSet.put(id, pattern);
                }
            }
        }
        else {
            pattern = pattern & ~bitPositionForAlias;

            if (pattern == targetBitPattern) {
                /*
                 * Put this Corr-Id into potential hit list. Wait for
                 * endExpulsions() to see if this Pattern makes it to the end.
                 */
                corrIdSessionHitList.add(id);
            }
            else {
                /*
                 * Withdraw from hit list (if it exists) as a Required Event
                 * also got expelled in this cycle along with a Not-Required
                 * Event.
                 */
                corrIdSessionHitList.remove(id);
            }

            if (pattern == 0) {
                workingPatternSet.remove(id);
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

        moveToStale();

        return results;
    }

    @Override
    public void eventArrived(String alias, Object id) {
        int bitPositionForAlias = aliasAndBitPatterns.get(alias);

        Integer pattern = workingPatternSet.get(id);
        if (pattern == null) {
            pattern = stalePatternSet.get(id);
            if (pattern == null) {
                pattern = 0;

                // New Pattern. Add it.
                handleWorkingSetArrival(id, bitPositionForAlias, pattern);
            }
            else {
                pattern = pattern | bitPositionForAlias;

                // Put it back. Nothing further to do.
                stalePatternSet.put(id, pattern);
            }
        }
        else {
            handleWorkingSetArrival(id, bitPositionForAlias, pattern);
        }
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
        }

        workingPatternSet.put(id, updatedPattern);
    }

    @Override
    public List<Object> endArrivals() {
        ArrayList<Object> results = new ArrayList<Object>(corrIdSessionHitList);

        moveToStale();

        return results;
    }

    private void moveToStale() {
        for (Iterator iter = corrIdSessionHitList.iterator(); iter.hasNext();) {
            Object staleId = iter.next();
            iter.remove();

            Integer pattern = workingPatternSet.remove(staleId);

            stalePatternSet.put(staleId, pattern);
        }
    }

    @Override
    public void endSession() {
    }
}
