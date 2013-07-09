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
package streamcruncher.innards.impl.expression;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import ognl.Node;
import ognl.Ognl;
import ognl.OgnlContext;
import streamcruncher.api.artifact.RowSpec;
import streamcruncher.boot.Registry;
import streamcruncher.innards.db.cache.CacheManager;
import streamcruncher.innards.db.cache.CachedData;
import streamcruncher.innards.expression.Constants;
import streamcruncher.innards.impl.query.DDLHelper;
import streamcruncher.util.LoggerManager;
import streamcruncher.util.RowEvaluator;
import streamcruncher.util.sysevent.SystemEvent;
import streamcruncher.util.sysevent.SystemEventBus;
import streamcruncher.util.sysevent.SystemEvent.Priority;

/*
 * Author: Ashwin Jayaprakash Date: Jun 17, 2007 Time: 8:32:02 PM
 */

public class OgnlRowEvaluator implements RowEvaluator {
    protected final String queryName;

    protected final String expressionStr;

    protected final OgnlRow ognlRow;

    protected final Node ognlExpression;

    protected final String[][] subQueryAndAlias;

    protected final Map<String, Object> ognlBatchContext;

    protected final CacheManager cacheManager;

    public OgnlRowEvaluator(String queryName, String expressionStr, RowSpec sourceRowSpec,
            Map<String, Object> context, Map<String, String> subQueries)
            throws ExpressionEvaluationException {
        this.queryName = queryName;
        this.expressionStr = expressionStr;

        try {
            this.ognlRow = new OgnlRow(sourceRowSpec);

            this.ognlRow.toggleTestMode();
            OgnlContext ognlContext = (OgnlContext) Ognl.createDefaultContext(this.ognlRow);
            this.ognlExpression = Ognl.compileExpression(ognlContext, this.ognlRow, expressionStr);
            this.ognlRow.toggleTestMode();
        }
        catch (Exception e) {
            throw new ExpressionEvaluationException(e);
        }

        this.ognlBatchContext = new HashMap<String, Object>();

        this.subQueryAndAlias = new String[subQueries == null ? 0 : subQueries.size()][];
        if (this.subQueryAndAlias.length > 0) {
            int l = 0;
            for (String alias : subQueries.keySet()) {
                this.subQueryAndAlias[l] = new String[] { subQueries.get(alias), alias };
                l++;
            }
        }

        this.cacheManager = Registry.getImplFor(CacheManager.class);
    }

    public String getExpressionStr() {
        return expressionStr;
    }

    public String getQueryName() {
        return queryName;
    }

    public void batchStart() {
        ognlBatchContext.clear();

        try {
            for (String[] sqlAndAlias : subQueryAndAlias) {
                CachedData data = cacheManager.getCachedData(sqlAndAlias[0]);
                Object obj = data.getData();
                ognlBatchContext.put(sqlAndAlias[1], obj);
            }
        }
        catch (Exception e) {
            ExpressionEvaluationException exception = new ExpressionEvaluationException(
                    "An error occurred while executing the Pre-Filter for the Query: " + queryName,
                    e);

            Logger logger = Registry.getImplFor(LoggerManager.class).getLogger(
                    OgnlRowEvaluator.class.getName());
            logger.log(Level.SEVERE, exception.getMessage(), exception);

            SystemEventBus bus = Registry.getImplFor(SystemEventBus.class);
            SystemEvent event = new SystemEvent(OgnlRowEvaluator.class.getName(),
                    RowEvaluator.class.getName(), exception, Priority.SEVERE);
            bus.submit(event);
        }
    }

    public ContextHolder rowStart(ContextHolder contextHolder, Object[] row) {
        OgnlContextHolder ognlContextHolder = (OgnlContextHolder) contextHolder;
        if (ognlContextHolder == null) {
            ognlContextHolder = new OgnlContextHolder();
        }

        Map<String, Object> mapContext = ognlContextHolder.getContext();
        // Populate the Context only once for each Row.
        if (mapContext.isEmpty()) {
            mapContext.put(Constants.VARIABLE_CURRENT_TIMESTAMP, new Timestamp(System
                    .currentTimeMillis()));
        }

        ognlRow.setRow(row);

        return ognlContextHolder;
    }

    public Object evaluate(ContextHolder contextHolder) {
        Object retVal = null;

        try {
            OgnlContextHolder holder = (OgnlContextHolder) contextHolder;
            Map<String, Object> mapContext = holder.getContext();

            for (String[] sqlAndAlias : subQueryAndAlias) {
                Object obj = ognlBatchContext.get(sqlAndAlias[1]);
                mapContext.put(sqlAndAlias[1], obj);
            }

            retVal = Ognl.getValue(ognlExpression, mapContext, ognlRow);
        }
        catch (Exception e) {
            ExpressionEvaluationException exception = new ExpressionEvaluationException(
                    "An error occurred while executing the Pre-Filter for the Query: " + queryName,
                    e);

            Logger logger = Registry.getImplFor(LoggerManager.class).getLogger(
                    OgnlRowEvaluator.class.getName());
            logger.log(Level.SEVERE, exception.getMessage(), exception);

            SystemEventBus bus = Registry.getImplFor(SystemEventBus.class);
            SystemEvent event = new SystemEvent(OgnlRowEvaluator.class.getName(),
                    RowEvaluator.class.getName(), exception, Priority.SEVERE);
            bus.submit(event);
        }

        return retVal;
    }

    public void rowEnd() {
    }

    public void batchEnd() {
        ognlBatchContext.clear();
    }

    // ----------------

    public static class OgnlContextHolder implements ContextHolder {
        protected final Map<String, Object> context;

        public OgnlContextHolder() {
            this.context = new HashMap<String, Object>();
        }

        public Map<String, Object> getContext() {
            return context;
        }

        public void clear() {
            context.clear();
        }

        public boolean isEmpty() {
            return context.isEmpty();
        }
    }

    public static class OgnlRow {
        protected final Class[] columnDefs;

        protected boolean testMode;

        protected Object[] row;

        /**
         * See {@link DDLHelper#getJavaTypes()}.
         * 
         * @param rowSpec
         */
        public OgnlRow(RowSpec rowSpec) {
            String[] types = rowSpec.getColumnNativeTypes();

            this.columnDefs = new Class[types.length];
            for (int i = 0; i < types.length; i++) {
                String[] array = types[i].split(RowSpec.INFO_SEPARATOR);

                try {
                    this.columnDefs[i] = Class.forName(array[0]);
                }
                catch (ClassNotFoundException e) {
                    String[] javaTypes = { java.lang.Integer.class.getName(),
                            java.lang.Float.class.getName(), java.lang.Double.class.getName(),
                            java.lang.Long.class.getName(), java.lang.String.class.getName(),
                            java.sql.Timestamp.class.getName() };

                    throw new RuntimeException("The Data type: " + array[0]
                            + " is not supported. Supported Types: " + Arrays.asList(javaTypes));
                }
            }
        }

        public void toggleTestMode() {
            testMode = !testMode;
        }

        public boolean isTestMode() {
            return testMode;
        }

        public void setRow(Object[] row) {
            this.row = row;
        }

        public void setColumn_$(int index, Object column) {
            row[index] = column;
        }

        public Integer getColumn_$Integer(int index) {
            if (testMode) {
                return 1;
            }

            return row[index] == null ? null : ((Number) row[index]).intValue();
        }

        public Long getColumn_$Long(int index) {
            if (testMode) {
                return 1L;
            }

            return row[index] == null ? null : ((Number) row[index]).longValue();
        }

        public Float getColumn_$Float(int index) {
            if (testMode) {
                return 1.0F;
            }

            return row[index] == null ? null : ((Number) row[index]).floatValue();
        }

        public Double getColumn_$Double(int index) {
            if (testMode) {
                return 1.0D;
            }

            return row[index] == null ? null : ((Number) row[index]).doubleValue();
        }

        public String getColumn_$String(int index) {
            if (testMode) {
                return "";
            }

            return (String) row[index];
        }

        public Timestamp getColumn_$Timestamp(int index) {
            if (testMode) {
                return new Timestamp(System.currentTimeMillis());
            }

            return (Timestamp) row[index];
        }

        public Object getColumn_$(int index) {
            if (testMode) {
                return new Object();
            }

            return row[index];
        }
    }
}