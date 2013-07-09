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

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/*
 * Author: Ashwin Jayaprakash Date: Aug 19, 2006 Time: 6:07:01 PM
 */

/**
 * Similar to what is done in {@link ScheduledThreadPoolExecutor}, to force the
 * use of a {@link DelayQueue} (<b>accepts only {@link DelayedRunnable}</b>)
 * into a {@link ThreadPoolExecutor}
 */
public class RunnableDelayQueue extends AbstractCollection<Runnable> implements
        BlockingQueue<Runnable> {
    protected final DelayQueue<DelayedRunnable> dq = new DelayQueue<DelayedRunnable>();

    public Runnable poll() {
        return dq.poll();
    }

    public Runnable peek() {
        return dq.peek();
    }

    public Runnable take() throws InterruptedException {
        return dq.take();
    }

    public Runnable poll(long timeout, TimeUnit unit) throws InterruptedException {
        return dq.poll(timeout, unit);
    }

    public boolean add(Runnable x) {
        return dq.add((DelayedRunnable) x);
    }

    public boolean offer(Runnable x) {
        return dq.offer((DelayedRunnable) x);
    }

    public void put(Runnable x) {
        dq.put((DelayedRunnable) x);
    }

    public boolean offer(Runnable x, long timeout, TimeUnit unit) {
        return dq.offer((DelayedRunnable) x, timeout, unit);
    }

    public Runnable remove() {
        return dq.remove();
    }

    public Runnable element() {
        return dq.element();
    }

    public void clear() {
        dq.clear();
    }

    public int drainTo(Collection<? super Runnable> c) {
        return dq.drainTo(c);
    }

    public int drainTo(Collection<? super Runnable> c, int maxElements) {
        return dq.drainTo(c, maxElements);
    }

    public int remainingCapacity() {
        return dq.remainingCapacity();
    }

    public boolean remove(Object x) {
        return dq.remove(x);
    }

    public boolean contains(Object x) {
        return dq.contains(x);
    }

    public int size() {
        return dq.size();
    }

    public boolean isEmpty() {
        return dq.isEmpty();
    }

    public Object[] toArray() {
        return dq.toArray();
    }

    public <T> T[] toArray(T[] array) {
        return dq.toArray(array);
    }

    public Iterator<Runnable> iterator() {
        return new Iterator<Runnable>() {
            private Iterator<DelayedRunnable> it = dq.iterator();

            public boolean hasNext() {
                return it.hasNext();
            }

            public Runnable next() {
                return it.next();
            }

            public void remove() {
                it.remove();
            }
        };
    }
}
