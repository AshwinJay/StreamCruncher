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
package streamcruncher.innards.core.partition.function;

import java.io.ObjectStreamException;

import streamcruncher.api.WindowSizeProvider;
import streamcruncher.api.artifact.RowSpec;
import streamcruncher.boot.ProviderManager;
import streamcruncher.boot.ProviderManagerException;
import streamcruncher.boot.Registry;
import streamcruncher.innards.util.SourceToTargetMapper;

/*
 * Author: Ashwin Jayaprakash Date: Feb 19, 2006 Time: 4:46:52 PM
 */

public abstract class MiscFunctionBuilder extends FunctionBuilder {
    protected final int[] sourceLocationForTargetCols;

    protected final int defaultSize;

    protected final String providerName;

    protected transient WindowSizeProvider provider;

    /**
     * @param selectedRowSpec
     * @param newRowSpec
     * @param sizeProvider
     */
    public MiscFunctionBuilder(RowSpec selectedRowSpec, RowSpec newRowSpec, int defaultSize,
            String providerName) throws ProviderManagerException {
        super(selectedRowSpec, newRowSpec);

        this.sourceLocationForTargetCols = new SourceToTargetMapper(selectedRowSpec, newRowSpec)
                .map();

        this.defaultSize = defaultSize;
        this.providerName = providerName;
        init();
    }

    private void init() throws ProviderManagerException {
        ProviderManager manager = Registry.getImplFor(ProviderManager.class);
        this.provider = (WindowSizeProvider) manager.createProvider(providerName);
        this.provider.setSize(this.defaultSize);
    }

    protected abstract Object readResolve() throws ObjectStreamException;

    public long getDefaultSize() {
        return defaultSize;
    }

    public String getProviderName() {
        return providerName;
    }

    public WindowSizeProvider getProvider() {
        return provider;
    }

    // ------------------

    public static class RandomFunctionBuilder extends MiscFunctionBuilder {
        private static final long serialVersionUID = 1L;

        public RandomFunctionBuilder(RowSpec selectedRowSpec, RowSpec newRowSpec, int defaultSize,
                String providerName) throws ProviderManagerException {
            super(selectedRowSpec, newRowSpec, defaultSize, providerName);
        }

        @Override
        protected Object readResolve() throws ObjectStreamException {
            Object object = null;

            try {
                object = new RandomFunctionBuilder(this.realTableRowSpec, this.finalTableRowSpec,
                        this.defaultSize, this.providerName);
            }
            catch (ProviderManagerException e) {
                ObjectStreamException e1 = new ObjectStreamException(e.getMessage()) {
                };
                e1.setStackTrace(e.getStackTrace());

                throw e1;
            }

            return object;
        }

        @Override
        public Function build(Object[] levelValues) {
            int size = provider.provideSize(levelValues);
            return new RandomFunction(realTableRowSpec, finalTableRowSpec, rowIdGenerator,
                    sourceLocationForTargetCols, size);
        }
    }

    public static class HighestFunctionBuilder extends MiscFunctionBuilder {
        private static final long serialVersionUID = 1L;

        protected final String columnName;

        protected final String[] updateGrpColumns;

        public HighestFunctionBuilder(RowSpec selectedRowSpec, RowSpec newRowSpec, int defaultSize,
                String providerName, String columnName, String[] updateGrpColumns)
                throws ProviderManagerException {
            super(selectedRowSpec, newRowSpec, defaultSize, providerName);

            this.columnName = columnName;
            this.updateGrpColumns = updateGrpColumns;
        }

        @Override
        protected Object readResolve() throws ObjectStreamException {
            Object object = null;

            try {
                object = new HighestFunctionBuilder(this.realTableRowSpec, this.finalTableRowSpec,
                        this.defaultSize, this.providerName, this.columnName, this.updateGrpColumns);
            }
            catch (ProviderManagerException e) {
                ObjectStreamException e1 = new ObjectStreamException(e.getMessage()) {
                };
                e1.setStackTrace(e.getStackTrace());

                throw e1;
            }

            return object;
        }

        @Override
        public Function build(Object[] levelValues) {
            int size = provider.provideSize(levelValues);
            return new SortedWindowFunction(realTableRowSpec, finalTableRowSpec, rowIdGenerator,
                    sourceLocationForTargetCols, size, columnName, updateGrpColumns,
                    SortedWindowFunction.Type.HIGHEST);
        }

        public String getColumnName() {
            return columnName;
        }
    }

    public static class LowestFunctionBuilder extends MiscFunctionBuilder {
        private static final long serialVersionUID = 1L;

        protected final String columnName;

        protected final String[] updateGrpColumns;

        public LowestFunctionBuilder(RowSpec selectedRowSpec, RowSpec newRowSpec, int defaultSize,
                String providerName, String columnName, String[] updateGrpColumns)
                throws ProviderManagerException {
            super(selectedRowSpec, newRowSpec, defaultSize, providerName);

            this.columnName = columnName;
            this.updateGrpColumns = updateGrpColumns;
        }

        @Override
        protected Object readResolve() throws ObjectStreamException {
            Object object = null;

            try {
                object = new LowestFunctionBuilder(this.realTableRowSpec, this.finalTableRowSpec,
                        this.defaultSize, this.providerName, this.columnName, this.updateGrpColumns);
            }
            catch (ProviderManagerException e) {
                ObjectStreamException e1 = new ObjectStreamException(e.getMessage()) {
                };
                e1.setStackTrace(e.getStackTrace());

                throw e1;
            }

            return object;
        }

        @Override
        public Function build(Object[] levelValues) {
            int size = provider.provideSize(levelValues);
            return new SortedWindowFunction(realTableRowSpec, finalTableRowSpec, rowIdGenerator,
                    sourceLocationForTargetCols, size, columnName, updateGrpColumns,
                    SortedWindowFunction.Type.LOWEST);
        }

        public String getColumnName() {
            return columnName;
        }
    }
}