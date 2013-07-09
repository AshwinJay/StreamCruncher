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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

import streamcruncher.api.artifact.RunningQuery;
import streamcruncher.kernel.PrioritizedSchedulableQuery;

/*
 * Author: Ashwin Jayaprakash Date: Jul 22, 2006 Time: 7:38:38 PM
 */
/**
 * This is the output of the Query parsing, produced by the Kernel which should
 * then be used to register with the Kernel. Before the registration, the
 * config-object can be obtained and the settings modified.
 */
public class ParsedQuery {
    protected final PrioritizedSchedulableQuery psq;

    protected ParsedQuery(PrioritizedSchedulableQuery psq) {
        this.psq = psq;
    }

    // --------------

    protected RunningQuery getRunningQuery() {
        return psq;
    }

    public QueryConfig getQueryConfig() {
        return psq.getQueryConfig();
    }

    public Collection<String> getCachedSubQueries() {
        Set<String> sqls = psq.getCachedSubQueries();
        return new ArrayList<String>(sqls);
    }
}
