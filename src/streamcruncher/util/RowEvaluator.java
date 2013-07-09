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
package streamcruncher.util;

import streamcruncher.innards.impl.expression.ExpressionEvaluationException;

/*
 * Author: Ashwin Jayaprakash Date: Jun 25, 2007 Time: 9:34:44 PM
 */

public interface RowEvaluator {
    public void batchStart();

    /**
     * This is useful if there are a bunch of Evaluators, all operating on the
     * same Source RowSpec, Context and same Row. The Context is created once
     * for the Row and is then populated and shared by all Rows.
     * 
     * @param contextHolder
     *            Can be <code>null</code>. If it is, then create a new one
     *            and return it.
     * @return The modified (same) or new Context.
     * @throws ExpressionEvaluationException
     */
    public ContextHolder rowStart(ContextHolder contextHolder, Object[] row)
            throws ExpressionEvaluationException;

    /**
     * @return Value after evaluation.
     * @throws ExpressionEvaluationException
     */
    public Object evaluate(ContextHolder contextHolder) throws ExpressionEvaluationException;

    public void rowEnd();

    public void batchEnd();

    public static interface ContextHolder {
        public Object getContext();

        public void clear();
    }
}