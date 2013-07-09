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

import java.io.Serializable;
import java.util.Map;

import streamcruncher.api.artifact.TableSpec;
import streamcruncher.innards.core.WhereClauseSpec;
import streamcruncher.innards.expression.Statement;

/*
 * Author: Ashwin Jayaprakash Date: Feb 13, 2007 Time: 1:28:34 PM
 */

public class CorrelationSpec implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * Source Table and List of "Src Column and Destination Column position"
     * arrays.
     */
    protected final Map<String, Integer[][]> sourceTblAndDestPosition;

    protected final Map<String, Integer> sourceTblAndCorrIdPosition;

    protected final Map<String, Integer> sourceTblAndRowIdPosition;

    protected final TableSpec outputTableSpec;

    protected final MatchSpec[] matchSpecs;

    protected Statement statement;

    protected WhereClauseSpec whereClauseSpec;

    public CorrelationSpec(Map<String, Integer[][]> sourceTblAndDestPosition,
            Map<String, Integer> sourceTblAndCorrIdPosition,
            Map<String, Integer> sourceTblAndRowIdPosition, TableSpec outputTableSpec,
            MatchSpec[] matchSpecs) {
        this.sourceTblAndDestPosition = sourceTblAndDestPosition;
        this.sourceTblAndCorrIdPosition = sourceTblAndCorrIdPosition;
        this.sourceTblAndRowIdPosition = sourceTblAndRowIdPosition;
        this.outputTableSpec = outputTableSpec;
        this.matchSpecs = matchSpecs;
    }

    public WhereClauseSpec getWhereClauseSpec() {
        return whereClauseSpec;
    }

    public void setWhereClauseSpec(WhereClauseSpec whereClauseSpec) {
        this.whereClauseSpec = whereClauseSpec;
    }

    public Statement getStatement() {
        return statement;
    }

    public void setStatement(Statement statement) {
        this.statement = statement;
    }

    public MatchSpec[] getMatchSpecs() {
        return matchSpecs;
    }

    public TableSpec getOutputTableSpec() {
        return outputTableSpec;
    }

    public Map<String, Integer[][]> getSourceTblAndDestPosition() {
        return sourceTblAndDestPosition;
    }

    public Map<String, Integer> getSourceTblAndCorrIdPosition() {
        return sourceTblAndCorrIdPosition;
    }

    public Map<String, Integer> getSourceTblAndRowIdPosition() {
        return sourceTblAndRowIdPosition;
    }

    public static class MatchSpec implements Serializable {
        private static final long serialVersionUID = 1L;

        protected final String[] presentAliases;

        protected final String[] notPresentAliases;

        public MatchSpec(String[] present, String[] notPresent) {
            this.presentAliases = present;
            this.notPresentAliases = notPresent;
        }

        public String[] getNotPresentAliases() {
            return notPresentAliases;
        }

        public String[] getPresentAliases() {
            return presentAliases;
        }
    }
}
