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
package streamcruncher.test.misc;

import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.testng.Assert;
import org.testng.annotations.Test;

import streamcruncher.innards.impl.expression.ExpressionEvaluationException;
import streamcruncher.test.TestGroupNames;
import streamcruncher.util.RowEvaluator;
import streamcruncher.util.TwoDAppendOnlyList;
import streamcruncher.util.TwoDAppendOnlyList.Reader;

/*
 * Author: Ashwin Jayaprakash Date: Jun 6, 2007 Time: 8:46:42 PM
 */

public class TwoDAppendOnlyListTest {
    @Test(groups = { TestGroupNames.SC_TEST })
    public void test() throws InterruptedException {
        final TwoDAppendOnlyList list = new TwoDAppendOnlyList(2);
        final int total = 40000;
        final AtomicBoolean stopFlag = new AtomicBoolean(false);
        final ConcurrentMap<Integer, Set<Integer>> results = new ConcurrentHashMap<Integer, Set<Integer>>();

        // ----------

        Thread producer = new Thread() {
            public void run() {
                TwoDAppendOnlyListTest.this.produce(list, total, stopFlag);
            }
        };

        Thread purger = new Thread() {
            public void run() {
                TwoDAppendOnlyListTest.this.purge(list, stopFlag);
            }
        };

        Thread[] readers = new Thread[5];
        for (int i = 0; i < readers.length; i++) {
            readers[i] = new Thread(i + "") {
                public void run() {
                    try {
                        Set<Integer> result = TwoDAppendOnlyListTest.this.read(list, stopFlag);
                        results.put(Integer.parseInt(getName()), result);
                    }
                    catch (NumberFormatException e) {
                        e.printStackTrace(System.err);
                    }
                    catch (ExpressionEvaluationException e) {
                        e.printStackTrace(System.err);
                    }
                }
            };
        }

        // ----------

        for (int i = 0; i < readers.length; i++) {
            readers[i].start();
        }
        Thread.sleep(1000);
        producer.start();
        purger.start();

        // ----------

        producer.join();
        Thread.sleep(5000);
        stopFlag.set(true);

        purger.join();
        for (int i = 0; i < readers.length; i++) {
            readers[i].join();
        }

        // ----------

        System.out.println("List size now: " + list.getSize() + ". Started with: " + total);

        for (Integer i : results.keySet()) {
            Set<Integer> r = results.get(i);

            System.out.println(i + ": " + r.size());

            Assert.assertEquals(r.size(), total, "Some items are missing");
        }
    }

    public int produce(TwoDAppendOnlyList list, final int total, AtomicBoolean stopFlag) {
        int k = 0;

        for (int i = 0; stopFlag.get() == false && i < total / 4; i++) {
            list.add(new Object[] { k++, "a" });
        }
        Thread.yield();

        for (int i = 0; stopFlag.get() == false && i < total / 4;) {
            int batch = Math.min(500, total / 4 - i);
            Object[][] items = new Object[batch][];

            for (int j = 0; stopFlag.get() == false && j < items.length; j++) {
                items[j] = new Object[] { k++, "b" };
            }
            list.add(items);

            i = i + items.length;
        }
        Thread.yield();

        for (int i = 0; stopFlag.get() == false && i < total / 4; i++) {
            list.add(new Object[] { k++, "c" });
        }
        Thread.yield();

        for (int i = 0; stopFlag.get() == false && i < total / 4;) {
            int batch = Math.min(500, total / 4 - i);
            Object[][] items = new Object[batch][];

            for (int j = 0; stopFlag.get() == false && j < items.length; j++) {
                items[j] = new Object[] { k++, "d" };
            }
            list.add(items);

            i = i + items.length;
        }

        return k;
    }

    public void purge(TwoDAppendOnlyList list, AtomicBoolean stopFlag) {
        while (stopFlag.get() == false) {
            list.removeUnusedOldFragments();
            Thread.yield();
        }
    }

    public Set<Integer> read(TwoDAppendOnlyList list, AtomicBoolean stopFlag)
            throws ExpressionEvaluationException {
        Set<Integer> results = new LinkedHashSet<Integer>();

        Reader reader = list.createReader();
        RowEvaluator filter = new RowEvaluator() {
            public void batchStart() {
            }

            public ContextHolder rowStart(ContextHolder contextHolder, Object[] row) {
                return new ContextHolder() {
                    public void clear() {
                    }

                    public Object getContext() {
                        return null;
                    }
                };
            }

            public Object evaluate(ContextHolder contextHolder) {
                return true;
            }

            public void rowEnd() {
            }

            public void batchEnd() {
            }
        };

        LinkedList<Object[]> buffer = new LinkedList<Object[]>();
        while (stopFlag.get() == false) {
            buffer.clear();
            reader.readInto(buffer, filter);
            for (Object[] objects : buffer) {
                results.add((Integer) objects[0]);
            }
        }

        return results;
    }
}
