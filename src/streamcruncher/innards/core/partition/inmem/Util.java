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
package streamcruncher.innards.core.partition.inmem;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import streamcruncher.innards.core.filter.TableFilter;
import streamcruncher.innards.core.partition.ChainedPartitioner;
import streamcruncher.innards.core.partition.EncapsulatedPartitioner;
import streamcruncher.innards.expression.Statement;
import streamcruncher.innards.impl.expression.ExpressionEvaluationException;
import streamcruncher.util.RowEvaluator;
import streamcruncher.util.RowEvaluator.ContextHolder;

/*
 * Author: Ashwin Jayaprakash Date: Sep 4, 2007 Time: 8:18:38 PM
 */

public class Util {
    public static List<InMemPartitionDataProducer> convertToList(TableFilter[] sources) {
        LinkedList<InMemPartitionDataProducer> inMemProducerList = new LinkedList<InMemPartitionDataProducer>();

        for (TableFilter filter : sources) {
            if (filter instanceof EncapsulatedPartitioner) {
                EncapsulatedPartitioner partitioner = (EncapsulatedPartitioner) filter;

                ChainedPartitioner ctp = partitioner.getChainedTablePartitioner();
                while (true) {
                    ChainedPartitioner tmp = ctp.getNextCTP();
                    if (tmp == null) {
                        break;
                    }

                    ctp = tmp;
                }

                if (ctp instanceof InMemPartitionDataProducer) {
                    inMemProducerList.add((InMemPartitionDataProducer) ctp);
                }
            }
            else if (filter instanceof InMemPartitioner) {
                inMemProducerList.add((InMemPartitionDataProducer) filter);
            }
        }

        return inMemProducerList;
    }

    /**
     * @param statement
     * @param rowFilter
     * @param columnEvaluators
     * @param rows
     * @return
     * @throws ExpressionEvaluationException
     */
    public static List<Object[]> filterRows(Statement statement, RowEvaluator rowFilter,
            RowEvaluator[] columnEvaluators, List<Object[]> rows)
            throws ExpressionEvaluationException {
        if (rowFilter != null) {
            rowFilter.batchStart();

            ContextHolder holder = null;
            for (Iterator<Object[]> iter = rows.iterator(); iter.hasNext();) {
                Object[] row = iter.next();

                holder = rowFilter.rowStart(holder, row);

                boolean b = (Boolean) rowFilter.evaluate(holder);
                if (b == false) {
                    iter.remove();
                }

                rowFilter.rowEnd();
                holder.clear();
            }

            rowFilter.batchEnd();
        }

        // --------------

        for (RowEvaluator evaluator : columnEvaluators) {
            evaluator.batchStart();
        }

        List<Object[]> results = rows;

        ContextHolder holder = null;
        if (columnEvaluators.length > 0 && rows.size() > 0) {
            results = new ArrayList<Object[]>(rows.size());

            for (Object[] sourceRow : rows) {
                Object[] resultRow = new Object[columnEvaluators.length];

                for (int i = 0; i < columnEvaluators.length; i++) {
                    holder = columnEvaluators[i].rowStart(holder, sourceRow);

                    resultRow[i] = columnEvaluators[i].evaluate(holder);

                    columnEvaluators[i].rowEnd();
                }

                holder.clear();
                results.add(resultRow);
            }
        }

        for (RowEvaluator evaluator : columnEvaluators) {
            evaluator.batchEnd();
        }

        // --------------

        // todo Post processing - First, Distinct, Group by etc.
        // Statement statement = correlationSpec.getStatement();
        // if (statement.getFirstX()) {
        // }

        return results;
    }
}
