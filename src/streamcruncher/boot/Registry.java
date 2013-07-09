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
package streamcruncher.boot;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

/*
 * Author: Ashwin Jayaprakash Date: Jan 1, 2006 Time: 11:10:34 AM
 */

/**
 * <b>Note:</b> This class is not Thread-safe.
 */
public class Registry {
    /**
     * We need to preserve the order in which the Components were added so that
     * the reverse order can be used to stop them.
     */
    protected final LinkedHashMap<Class<? extends Component>, Component> componentImplMap;

    protected Registry() {
        componentImplMap = new LinkedHashMap<Class<? extends Component>, Component>();
    }

    protected <T extends Component> Component getComponentImpl(Class<T> componentClass) {
        return componentImplMap.get(componentClass);
    }

    /**
     * @param <T>
     * @param componentClass
     * @param componentImpl
     */
    public <T extends Component> void setComponentImpl(Class<T> componentClass, T componentImpl) {
        /*
         * Check if the method invoking this has the necessary Credentials.
         */

        componentImplMap.put(componentClass, componentImpl);
    }

    /**
     * @return Returns all the registered components in the reverse order that
     *         they were added to the Registry (LIFO). <b>Note:</b> A new List
     *         is created everytime this method is invoked and that List is
     *         returned.
     */
    protected List<Component> getAllRegisteredComponents() {
        ArrayList<Component> list = new ArrayList<Component>(componentImplMap.values());
        Collections.reverse(list);

        return list;
    }

    // ------------------- static fields and methods below -------------------

    protected static Registry registry = null;

    public static void init() {
        getInstance();
    }

    public static Registry getInstance() {
        if (registry == null) {
            registry = new Registry();
        }

        return registry;
    }

    /**
     * @param clazz
     * @return Instance of type clazz
     */
    public static <V extends Component> V getImplFor(Class<V> clazz) {
        Component component = registry.getComponentImpl(clazz);

        return clazz.cast(component);
    }

    public static void discard() {
        registry = null;
    }
}