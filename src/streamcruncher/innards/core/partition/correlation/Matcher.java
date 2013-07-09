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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/*
 * Author: Ashwin Jayaprakash Date: Feb 26, 2007 Time: 6:39:43 PM
 */

public abstract class Matcher {
    protected final String[] presentAliases;

    protected final String[] notPresentAliases;

    protected final Map<String, Integer> aliasAndBitPatterns;

    protected final int targetBitPattern;

    protected final int notPresentBitPattern;

    /**
     * The current map of Correlation Id and Patterns.
     */
    protected final Map<Object, Integer> workingPatternSet;

    /**
     * The map of Correlation Ids and Patterns that have already fired.
     */
    protected final Map<Object, Integer> stalePatternSet;

    protected final Set<Object> corrIdSessionHitList;

    public Matcher(String[] presentAliases, String[] notPresentAliases) {
        this.presentAliases = presentAliases;
        this.notPresentAliases = notPresentAliases;

        this.aliasAndBitPatterns = new HashMap<String, Integer>();

        int c = 0;
        int notExpected = 0;
        for (int i = 0; i < notPresentAliases.length; i++) {
            aliasAndBitPatterns.put(notPresentAliases[i], (1 << c));
            notExpected = notExpected | (1 << c);
            c++;
        }
        this.notPresentBitPattern = notExpected;

        int expected = 0;
        for (int i = 0; i < presentAliases.length; i++) {
            aliasAndBitPatterns.put(presentAliases[i], (1 << c));
            expected = expected | (1 << c);
            c++;
        }
        this.targetBitPattern = expected;

        this.workingPatternSet = new HashMap<Object, Integer>();
        this.stalePatternSet = new HashMap<Object, Integer>();

        this.corrIdSessionHitList = new HashSet<Object>();
    }

    public Map<String, Integer> getAliasAndBitPatterns() {
        return aliasAndBitPatterns;
    }

    public int getNotPresentBitPattern() {
        return notPresentBitPattern;
    }

    public Map<Object, Integer> getStalePatternSet() {
        return stalePatternSet;
    }

    public int getTargetBitPattern() {
        return targetBitPattern;
    }

    public Map<Object, Integer> getWorkingPatternSet() {
        return workingPatternSet;
    }

    public String[] getNotPresentAliases() {
        return notPresentAliases;
    }

    public String[] getPresentAliases() {
        return presentAliases;
    }

    // ----------

    public abstract void startSession();

    public abstract void eventExpelled(String alias, Object id);

    public abstract List<Object> endExpulsions();

    public abstract void eventArrived(String alias, Object id);

    protected abstract void handleWorkingSetArrival(Object id, int bitPositionForAlias,
            Integer pattern);

    public abstract List<Object> endArrivals();

    public abstract void endSession();
}
