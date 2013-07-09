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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import streamcruncher.boot.Registry;

/*
 * Author: Ashwin Jayaprakash Date: Jan 7, 2006 Time: 10:28:43 AM
 */

/**
 * This Class will work correctly when there is only one Producer Thread and one
 * Consumer Thread operating on it at any instant. There is no need for any
 * synchronization in such cases.
 */
public class AppendOnlyPrimitiveLongList implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final long[] EMPTY_LONG_ARRAY = {};

    public static final int FRAGMENT_SIZE = 512;

    public static final int WAIT_SCAN_TIME_MSECS = 300;

    protected final int fragmentSize;

    protected final AtomicInteger size;

    protected volatile Fragment head;

    protected volatile Fragment tail;

    /**
     * Uses a default fragment size of {@link #FRAGMENT_SIZE}.
     */
    public AppendOnlyPrimitiveLongList() {
        this(FRAGMENT_SIZE);
    }

    public AppendOnlyPrimitiveLongList(int fragmentSize) {
        this.fragmentSize = fragmentSize;
        this.size = new AtomicInteger(0);
        this.head = createFragment(fragmentSize);
        this.tail = this.head;
    }

    // --------------------------

    protected Fragment createFragment(int fragSize) {
        return new Fragment(fragSize);
    }

    // --------------------------

    protected void moveHead() {
        Fragment newHead = head.getNext();
        head.discard();
        newHead.setPrev(null);

        head = newHead;
    }

    /**
     * @return Returns the number of items available.
     */
    public int getSize() {
        return size.get();
    }

    /**
     * @return Returns the fragmentSize.
     */
    public int getFragmentSize() {
        return fragmentSize;
    }

    // --------------------------

    public void add(long item) {
        if (tail.add(item) == false) {
            Fragment fragment = createFragment(fragmentSize);
            fragment.setPrev(tail);
            tail.setNext(fragment);

            tail = fragment;
            tail.add(item);
        }

        size.incrementAndGet();
    }

    public void add(long[] items) {
        int copyStartPos = 0;

        while (copyStartPos < items.length) {
            int consumed = tail.add(items, copyStartPos);
            size.addAndGet(consumed);
            copyStartPos = copyStartPos + consumed;

            if (consumed == 0) {
                Fragment fragment = createFragment(fragmentSize);
                fragment.setPrev(tail);
                tail.setNext(fragment);

                tail = fragment;
            }
        }
    }

    /**
     * Non-blobking method call.
     * 
     * @return <code>null</code> if there are no items available. Or, returns
     *         the first item that can be read, This does not alter the state of
     *         the "List".
     */
    public Long peek() {
        if (size.get() == 0) {
            return null;
        }

        // --------------------------

        Long retVal = null;

        try {
            retVal = head.peek();
        }
        catch (NothingLeftToRemoveException e) {
            if (head != tail) {
                try {
                    Fragment fragment = head.getNext();
                    retVal = fragment.peek();
                }
                catch (NothingLeftToRemoveException e1) {
                    // Can't happen. But log anyway.
                    Logger logger = Registry.getImplFor(LoggerManager.class).getLogger(
                            AppendOnlyPrimitiveLongList.class.getName());
                    logger.log(Level.WARNING, "Unexpected Exception: 'head.getNext().peek()'", e1);
                }
            }
        }

        return retVal;
    }

    /**
     * Uses default {@link AppendOnlyPrimitiveLongList#WAIT_SCAN_TIME_MSECS}
     * wait time when item is not available.
     * 
     * @see #remove(long)
     * @return The first available item.
     */
    public long remove() {
        return remove(WAIT_SCAN_TIME_MSECS);
    }

    /**
     * Threads keeps waiting until an item becomes available for removal.
     * 
     * @param waitTimeMsecs
     *            The time in milliseconds that the Thread must wait before
     *            checking if an item is available for removal.
     * @return The first available item.
     */
    public long remove(long waitTimeMsecs) {
        long retVal;

        try {
            retVal = head.remove(waitTimeMsecs);
            size.decrementAndGet();
        }
        catch (NothingLeftToRemoveException e) {
            while (head == tail) {
                synchronized (head) {
                    try {
                        head.wait(waitTimeMsecs);
                    }
                    catch (InterruptedException e1) {
                        Logger logger = Registry.getImplFor(LoggerManager.class).getLogger(
                                AppendOnlyPrimitiveLongList.class.getName());
                        logger.log(Level.WARNING, e1.getMessage(), e1);
                    }
                }
            }

            moveHead();
            retVal = remove(waitTimeMsecs);
        }

        return retVal;
    }

    /**
     * This method returns an array of items currently available in the first
     * Fragment. Or, an empty-array if nothing is currently available. The size
     * of the array returned is smaller than the number indicated by
     * {@link #getSize()}. Since this Class maintains a list of Fragments, this
     * method returns all the contents available in the first Fragment in the
     * chain. Subsequent invocations will return the contents of the other
     * Fragments in-order in which they appear in the chain.
     * 
     * @param maxLength
     *            The maximum available items that must be returned. This
     *            parameter cannot be greater than the Fragment-size returned by
     *            {@link #getFragmentSize()}.
     * @return
     */
    public long[] removeAvailable(int maxLength) {
        long[] retVal = EMPTY_LONG_ARRAY;

        try {
            retVal = head.removeAvailable(maxLength);
        }
        catch (NothingLeftToRemoveException e) {
            if (head != tail) {
                moveHead();
                try {
                    retVal = head.removeAvailable(maxLength);
                }
                catch (NothingLeftToRemoveException e1) {
                    // Can't happen.
                    throw new RuntimeException("Unexpected End-of-List", e1);
                }
            }
        }

        size.addAndGet(-retVal.length);

        return retVal;
    }

    /**
     * Returns the items available in the first Fragment.
     * 
     * @return Maximum size returned will be equal to {@link #getFragmentSize()}
     * @see #removeAvailable(int)
     */
    public long[] removeAvailable() {
        return removeAvailable(getFragmentSize());
    }

    // --------------------------

    protected static class NothingLeftToRemoveException extends Exception {
        private static final long serialVersionUID = 1L;
    }

    protected static class Fragment implements Serializable {
        private static final long serialVersionUID = 1L;

        private final Lock lock;

        private final Condition condition;

        /**
         * Since this exception keeps getting thrown over and over when the
         * Reader Thread keeps trying to remove all the items in the Chain of
         * Fragments, and not really for an Error condition, it's less expensive
         * to create it once and re-use it.
         */
        protected final NothingLeftToRemoveException endMarker;

        protected volatile Fragment prev;

        protected final long[] data;

        protected volatile Fragment next;

        /**
         * Points to the position that has been read so far. -1, if nothing has
         * been read.
         */
        protected volatile int readIndex;

        /**
         * Points to the first empty slot to which a new item has to be added.
         */
        protected volatile int fillIndex;

        protected Fragment(int fragmentSize) {
            lock = new ReentrantLock();
            condition = lock.newCondition();

            endMarker = new NothingLeftToRemoveException();
            StackTraceElement[] elements = endMarker.getStackTrace();
            elements = new StackTraceElement[] { elements[0] };
            endMarker.setStackTrace(elements);

            data = new long[fragmentSize];
            readIndex = -1;
            fillIndex = 0;
        }

        // ----------------------

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
        public void setNext(Fragment next) {
            this.next = next;
        }

        /**
         * @return Returns the prev.
         */
        public Fragment getPrev() {
            return prev;
        }

        /**
         * @param prev
         *            The prev to set.
         */
        public void setPrev(Fragment prev) {
            this.prev = prev;
        }

        // ----------------------

        /**
         * @param item
         * @return <code>true</code> if Fragment has room to add this item,
         *         <code>false</code> otherwise.
         */
        public boolean add(long item) {
            if (fillIndex == data.length) {
                return false;
            }

            data[fillIndex] = item;
            fillIndex++;

            return true;
        }

        /**
         * @param items
         * @return The number of items that were copied from the Array provided.
         *         <code>0</code> if the {@link Fragment} is full.
         */
        public int add(long[] items, int startPos) {
            int toCopy = (data.length - fillIndex);
            int tmp = (items.length - startPos);
            toCopy = Math.min(toCopy, tmp);
            if (toCopy == 0) {
                return toCopy;
            }

            System.arraycopy(items, startPos, data, fillIndex, toCopy);
            fillIndex = fillIndex + toCopy;

            return toCopy;
        }

        /**
         * @return The first item available. Does not change the state. Just
         *         peeks.
         * @throws NothingLeftToRemoveException
         *             When this fragment has no items to remove.
         */
        public long peek() throws NothingLeftToRemoveException {
            final int fillIdxSnapshot = fillIndex;
            final int readIdxSnapshot = readIndex;

            if (readIdxSnapshot == fillIdxSnapshot - 1) {
                if (fillIdxSnapshot == data.length) {
                    throw endMarker;
                }
            }

            return data[readIdxSnapshot + 1];
        }

        /**
         * @param spinWaitTimeMsecs
         *            The time in Milliseconds to wait before checking again
         *            (wait-check-wait loop) if an item is available for
         *            removal.
         * @return Returns the first item that is available for removal. If
         *         there is nothing currently available, then the Thread blocks
         *         until an item is added.
         * @throws NothingLeftToRemoveException
         *             If this Fragment has already been completely read and
         *             removed.
         */
        public long remove(long spinWaitTimeMsecs) throws NothingLeftToRemoveException {
            final int fillIdxSnapshot = fillIndex;

            if (readIndex == fillIdxSnapshot - 1) {
                if (fillIdxSnapshot == data.length) {
                    throw endMarker;
                }

                lock.lock();
                try {
                    while (readIndex == fillIndex - 1) {
                        try {
                            condition.await(spinWaitTimeMsecs, TimeUnit.MILLISECONDS);
                        }
                        catch (InterruptedException e) {
                            Logger logger = Registry.getImplFor(LoggerManager.class).getLogger(
                                    AppendOnlyPrimitiveLongList.class.getName());
                            logger.log(Level.WARNING, e.getMessage(), e);
                        }
                    }
                }
                finally {
                    lock.unlock();
                }
            }

            return data[++readIndex];
        }

        /**
         * @param maxLength
         *            The maximum items that must be returned or less. Cannot
         *            exceed the Fragment's size.
         * @return Returns an array containing the items that haven't been
         *         removed before. Returns an empty-array if there is nothing
         *         currently available to removal.
         * @throws NothingLeftToRemoveException
         *             If this Fragment has already been completely read.
         */
        public long[] removeAvailable(int maxLength) throws NothingLeftToRemoveException {
            final int fillIdxSnapshot = fillIndex;

            if (readIndex == fillIdxSnapshot - 1) {
                if (fillIdxSnapshot == data.length) {
                    throw endMarker;
                }

                return EMPTY_LONG_ARRAY;
            }

            int resultLen = fillIdxSnapshot - readIndex - 1;
            long[] copy = null;
            if (resultLen == data.length && maxLength >= data.length) {
                // Reuse the whole array.
                copy = data;
            }
            else {
                resultLen = Math.min(resultLen, maxLength);
                copy = new long[resultLen];
                System.arraycopy(data, readIndex + 1, copy, 0, copy.length);
            }

            readIndex += copy.length;

            return copy;
        }

        public void discard() {
            fillIndex = data.length;
            readIndex = data.length - 1;

            prev = null;
            next = null;
        }
    }
}
