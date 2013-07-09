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

import java.io.Serializable;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import streamcruncher.innards.impl.expression.ExpressionEvaluationException;
import streamcruncher.util.RowEvaluator.ContextHolder;

/*
 * Author: Ashwin Jayaprakash Date: Jun 5, 2007 Time: 7:48:46 PM
 */

/**
 * Data structure optimized for append only operations. Append must be performed
 * by one Thread only at any time. Readers may be concurrent and will start from
 * the beginning of the list and move towards the end. Another Thread can purge
 * the list from the beginning, concurrently. Purging is done only upto the
 * first Fragment from the beginning, that is being read by a Reader.
 */
public class TwoDAppendOnlyList implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * Each Fragment's capacity: {@value}.
     */
    public static final int FRAGMENT_CAPACITY = 1024;

    protected final int fragmentCapacity;

    protected final ReentrantReadWriteLock listLock;

    protected final WriteLock listModLock;

    protected final ReadLock listTraverseLock;

    protected final AtomicLong size;

    protected volatile Fragment head;

    protected volatile Fragment tail;

    /**
     * Uses a default fragment size of {@link #FRAGMENT_CAPACITY}.
     */
    public TwoDAppendOnlyList() {
        this(FRAGMENT_CAPACITY);
    }

    public TwoDAppendOnlyList(int fragmentCapacity) {
        this.fragmentCapacity = fragmentCapacity;

        this.listLock = new ReentrantReadWriteLock();
        this.listModLock = this.listLock.writeLock();
        this.listTraverseLock = this.listLock.readLock();

        this.size = new AtomicLong(0);
        this.head = new Fragment(fragmentCapacity);
        this.tail = this.head;
    }

    protected static class Fragment implements Serializable {
        private static final long serialVersionUID = 1L;

        protected final ReentrantReadWriteLock readWriteLock;

        protected final ReadLock readLock;

        protected final AtomicInteger readers;

        protected final WriteLock purgeLock;

        protected final int capacity;

        protected volatile Fragment prev;

        protected final Object[][] data;

        protected volatile Fragment next;

        /**
         * Points to the first empty slot to which a new item has to be added.
         */
        protected AtomicInteger fillIndex;

        protected Fragment(int fragmentCapacity) {
            this.readWriteLock = new ReentrantReadWriteLock();
            this.readLock = this.readWriteLock.readLock();
            this.purgeLock = this.readWriteLock.writeLock();
            this.readers = new AtomicInteger(0);

            this.capacity = fragmentCapacity;
            this.data = new Object[fragmentCapacity][];
            this.fillIndex = new AtomicInteger(0);
        }

        // ------------

        /**
         * @return Returns the next.
         */
        public Fragment getNext() {
            return next;
        }

        /**
         * @param next
         *            The next to set.
         */
        protected void setNext(Fragment next) {
            this.next = next;
        }

        /**
         * @return Returns the prev.
         */
        protected Fragment getPrev() {
            return prev;
        }

        /**
         * @param prev
         *            The prev to set.
         */
        public void setPrev(Fragment prev) {
            this.prev = prev;
        }

        public int getCapacity() {
            return capacity;
        }

        public int getSize() {
            return fillIndex.get();
        }

        public ReadLock getReadLock() {
            return readLock;
        }

        public WriteLock getPurgeLock() {
            return purgeLock;
        }

        public AtomicInteger getReaders() {
            return readers;
        }

        // ------------

        /**
         * @param item
         * @return <code>true</code> if Fragment has room to add this item,
         *         <code>false</code> otherwise.
         */
        public boolean add(Object[] item) {
            if (fillIndex.get() == data.length) {
                return false;
            }

            data[fillIndex.get()] = item;
            fillIndex.incrementAndGet();

            return true;
        }

        /**
         * @param items
         * @return The number of items that were copied from the Array provided.
         *         <code>0</code> if the {@link Fragment} is full.
         */
        public int add(Object[][] items, int startPos) {
            int toCopy = (data.length - fillIndex.get());
            int tmp = (items.length - startPos);
            toCopy = Math.min(toCopy, tmp);
            if (toCopy == 0) {
                return toCopy;
            }

            System.arraycopy(items, startPos, data, fillIndex.get(), toCopy);
            fillIndex.addAndGet(toCopy);

            return toCopy;
        }

        /**
         * @param from
         *            The position in the internal buffer to read from -
         *            <code>0 to {@link Fragment#capacity}</code>.
         * @param dest
         *            The queue to which the data should be added.
         * @param itemsAddedCounter
         *            Adds the number of items that were added to the Queue -
         *            i.e the number of items that made it through the Filter.
         * @param filter
         *            Can be <code>null</code>.
         * @return The number of items that were read, <b>irrespective</b> of
         *         the number that made it through the filter.
         * @throws ExpressionEvaluationException
         */
        public int readInto(int from, Queue<Object[]> dest, AtomicInteger itemsAddedCounter,
                RowEvaluator filter) throws ExpressionEvaluationException {
            int c = 0;
            int limit = fillIndex.get();

            if (filter != null) {
                filter.batchStart();
            }

            ContextHolder holder = null;
            for (int i = from; i < limit; i++) {
                Object[] item = data[i];

                if (filter != null) {
                    holder = filter.rowStart(holder, item);
                }

                if (filter == null || ((Boolean) filter.evaluate(holder)).booleanValue() == true) {
                    dest.offer(item);
                    itemsAddedCounter.incrementAndGet();
                }

                c++;

                if (filter != null) {
                    filter.rowEnd();
                    holder.clear();
                }
            }

            if (filter != null) {
                filter.batchEnd();
            }

            return c;
        }
    }

    // ------------

    public void add(Object[] item) {
        boolean b = tail.add(item);
        if (b == false) {
            addTrailingFragment();
            tail.add(item);
        }

        size.incrementAndGet();
    }

    public void add(Object[][] items) {
        int start = 0;

        while (start < items.length) {
            int c = tail.add(items, start);
            if (c == 0) {
                addTrailingFragment();
            }
            start = start + c;
            size.addAndGet(c);
        }
    }

    protected void addTrailingFragment() {
        listModLock.lock();
        try {
            Fragment fragment = new Fragment(fragmentCapacity);
            tail.setNext(fragment);
            fragment.setPrev(tail);
            tail = fragment;
        }
        finally {
            listModLock.unlock();
        }
    }

    public void removeUnusedOldFragments() {
        Fragment fragment = head;
        while (fragment != null) {
            if (fragment.getReaders().get() > 0) {
                break;
            }

            WriteLock fragmentPurgeLock = fragment.getPurgeLock();
            if (fragmentPurgeLock.tryLock()) {
                Fragment nextHead = null;

                try {
                    listModLock.lock();
                    try {
                        nextHead = fragment.getNext();
                        fragment.setNext(null);

                        if (nextHead == null /* head == tail */) {
                            head = new Fragment(fragmentCapacity);
                            tail = head;
                        }
                        else {
                            nextHead.setPrev(null);
                            head = nextHead;
                        }

                        size.addAndGet(-1 * fragment.getSize());
                    }
                    finally {
                        listModLock.unlock();
                    }
                }
                finally {
                    fragmentPurgeLock.unlock();
                }

                fragment = nextHead;
            }
            else {
                break;
            }
        }
    }

    public Fragment getHead() {
        return head;
    }

    public int getFragmentCapacity() {
        return fragmentCapacity;
    }

    public long getSize() {
        return size.get();
    }

    public WriteLock getListModLock() {
        return listModLock;
    }

    public ReadLock getListTraverseLock() {
        return listTraverseLock;
    }

    public Reader createReader() {
        return new Reader(this);
    }

    // ------------

    public static class Reader {
        protected final TwoDAppendOnlyList list;

        protected final AtomicInteger bufferAddCounter;

        protected Fragment fragment;

        protected int fragmentCapacity;

        protected int fragmentReadCount;

        protected Reader(TwoDAppendOnlyList list) {
            this.fragment = getAndLockHead(list);
            this.fragmentCapacity = this.fragment.getCapacity();
            this.fragmentReadCount = 0;

            this.list = list;
            this.bufferAddCounter = new AtomicInteger();
        }

        protected Fragment getAndLockHead(TwoDAppendOnlyList list) {
            Fragment f = null;

            while (true) {
                f = list.getHead();

                ReadLock readLock = f.getReadLock();
                readLock.lock();
                try {
                    /*
                     * By the time we acquired this Read-lock, the Fragment
                     * might already have been purged.
                     */
                    if (f != list.getHead()) {
                        continue;
                    }

                    f.getReaders().incrementAndGet();
                    break;
                }
                finally {
                    readLock.unlock();
                }
            }

            return f;
        }

        /**
         * @return true if the move succeeded. Otherwise, it means that there
         *         was no new Fragment to move to.
         */
        protected boolean moveToNextFragment() {
            Fragment f = null;
            try {
                list.getListTraverseLock().lock();
                f = fragment.getNext();
            }
            finally {
                list.getListTraverseLock().unlock();
            }

            if (f == null) {
                return false;
            }

            // ------------

            f.getReaders().incrementAndGet();
            fragment.getReaders().decrementAndGet();

            fragment = f;
            fragmentCapacity = fragment.getCapacity();
            fragmentReadCount = 0;

            return true;
        }

        /**
         * @param copyBuffer
         *            The buffer to read the data into.
         * @param filter
         *            The filter to use while adding items into the buffer. Can
         *            be <code>null</code>.
         * @return The number of items that were added to the Buffer - i.e the
         *         number of Items that made it through the Filter.
         * @throws ExpressionEvaluationException
         */
        public int readInto(Queue<Object[]> copyBuffer, RowEvaluator filter)
                throws ExpressionEvaluationException {
            bufferAddCounter.set(0);

            /*
             * Try to read the current Fragment. If we reach the end, then try
             * to move to the next fragment and read that too. This is needed,
             * if the current Fragment has just the last few items and there is
             * a next fragment that has lot of items. So, in the interest of
             * performance, it's better to try and read the next fragment too,
             * while we are here.
             */
            for (int i = 0; i < 2; i++) {
                if (fragmentReadCount == fragmentCapacity) {
                    if (moveToNextFragment() == false) {
                        break;
                    }
                }

                int currentSize = fragment.getSize();
                int diff = currentSize - fragmentReadCount;

                int numItemsChecked = 0;
                if (currentSize > 0 && diff > 0) {
                    numItemsChecked = fragment.readInto(fragmentReadCount, copyBuffer,
                            bufferAddCounter, filter);
                }
                fragmentReadCount = fragmentReadCount + numItemsChecked;
            }

            return bufferAddCounter.get();
        }
    }
}
