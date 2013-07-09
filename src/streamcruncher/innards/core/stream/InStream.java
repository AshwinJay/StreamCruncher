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
package streamcruncher.innards.core.stream;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import streamcruncher.api.artifact.RowSpec;
import streamcruncher.api.artifact.RunningQuery;
import streamcruncher.api.artifact.TableFQN;
import streamcruncher.boot.Registry;
import streamcruncher.innards.core.EventBucket;
import streamcruncher.innards.core.InstreamNotificationRendezvous;
import streamcruncher.innards.core.filter.FilteredTable;
import streamcruncher.innards.core.partition.PartitionedTable;
import streamcruncher.innards.db.DatabaseInterface;
import streamcruncher.util.TwoDAppendOnlyList;

/*
 * Author: Ashwin Jayaprakash Date: Jan 2, 2006 Time: 10:20:27 AM
 */

public class InStream implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * Although a Table never gets created, for the sake of being consistent
     * with {@linkplain FilteredTable Filters} and
     * {@linkplain  PartitionedTable Partitions}, the InStream gets created
     * with the Schema from {@link DatabaseInterface#getSchema()}.
     */
    protected final String schema;

    protected final String name;

    protected final RowSpec rowSpec;

    protected final int blockSize;

    /**
     * Map of {@link RunningQuery#getName()} - {@link InStreamListenerData}.
     */
    protected final ConcurrentMap<String, InStreamListenerData> listeners;

    protected final TwoDAppendOnlyList streamData;

    protected final InstreamNotificationRendezvous notificationRendezvous;

    // --------------------------

    protected final int hashCode;

    protected final String str;

    // --------------------------

    protected final AtomicBoolean notificationLock;

    /**
     * @param name
     * @param rowSpec
     * @param blockSize
     * @param notificationRendezvous
     */
    public InStream(String name, RowSpec rowSpec, int blockSize,
            InstreamNotificationRendezvous notificationRendezvous) {
        DatabaseInterface dbInterface = Registry.getImplFor(DatabaseInterface.class);
        this.schema = dbInterface.getSchema();
        this.name = name;
        this.rowSpec = rowSpec;
        this.blockSize = blockSize;

        this.listeners = new ConcurrentHashMap<String, InStreamListenerData>();
        this.streamData = new TwoDAppendOnlyList(blockSize);
        this.notificationRendezvous = notificationRendezvous;

        this.notificationLock = new AtomicBoolean(false);

        // --------------------------

        int hash = (schema + ".").hashCode();
        hash = hash + (37 * (name + " ").hashCode());
        this.hashCode = hash;

        this.str = schema + "." + name;
    }

    /**
     * @return Same instance, but empty {@link #listeners}.
     * @throws ObjectStreamException
     */
    protected Object writeReplace() throws ObjectStreamException {
        this.listeners.clear();
        return this;
    }

    // --------------------------

    /**
     * @return Returns the name.
     */
    public String getName() {
        return name;
    }

    /**
     * @return Returns the schema.
     */
    public String getSchema() {
        return schema;
    }

    public String getFQN() {
        return (schema == null) ? name : str;
    }

    public RowSpec getRowSpec() {
        return rowSpec;
    }

    public int getBlockSize() {
        return blockSize;
    }

    /**
     * @return Returns the listenerData.
     */
    public ConcurrentMap<String, InStreamListenerData> getListeners() {
        return listeners;
    }

    public boolean attemptNotificationLock() {
        return notificationLock.compareAndSet(false, true);
    }

    public boolean isNotificationLockSet() {
        return notificationLock.get();
    }

    public void unsetNotificationLock() {
        notificationLock.set(false);
    }

    /**
     * @return Returns the list of Events.
     */
    public TwoDAppendOnlyList getStreamData() {
        return streamData;
    }

    // --------------------------

    public void afterRegisteringRQ(RunningQuery runningQuery) {
        LinkedList<EventBucket> bucketsOnCurrStream = null;

        EventBucket[] eventBuckets = runningQuery.getEventBuckets();
        for (EventBucket bucket : eventBuckets) {
            bucketsOnCurrStream = addAsListener(bucketsOnCurrStream, bucket);
        }

        if (bucketsOnCurrStream != null) {
            EventBucket[] thisStreamsBuckets = bucketsOnCurrStream
                    .toArray(new EventBucket[bucketsOnCurrStream.size()]);

            for (EventBucket bucket : thisStreamsBuckets) {
                bucket.setEventBucketClient(runningQuery);
                bucket.setStreamDataBuffer(getStreamData());
            }

            InStreamListenerData listenerData = new InStreamListenerData(runningQuery.getName(),
                    thisStreamsBuckets);
            listeners.put(runningQuery.getName(), listenerData);
        }
    }

    protected LinkedList<EventBucket> addAsListener(LinkedList<EventBucket> bucketsOnCurrStream,
            EventBucket bucket) {
        TableFQN currTableFQN = bucket.getSourceTableFQN();

        String thisFQN = this.getFQN();
        String thatFQN = currTableFQN.getFQN();

        // Aliases will be ignored. We just need the FQN.
        if (thisFQN.equals(thatFQN)) {
            if (bucketsOnCurrStream == null) {
                bucketsOnCurrStream = new LinkedList<EventBucket>();
            }

            bucketsOnCurrStream.add(bucket);
        }

        return bucketsOnCurrStream;
    }

    public void beforeUnregisteringRQ(RunningQuery runningQuery) {
        listeners.remove(runningQuery.getName());
    }

    // --------------------------

    /**
     * <b>Note:</b> This operation works by assuming that there will be only
     * one Thread invoking this method at a time.
     * 
     * @param event
     */
    public void addEvent(Object[] event) {
        // todo Verify event array is not null, column size is expected etc.

        streamData.add(event);

        if (isNotificationLockSet() == false) {
            notificationRendezvous.sendNotification(this);
        }
    }

    /**
     * <b>Note:</b> This operation works by assuming that there will be only
     * one Thread invoking this method at a time.
     * 
     * @param events
     */
    public void addEvents(Object[][] events) {
        // todo Verify event array is not null, column size is expected etc.

        streamData.add(events);

        if (isNotificationLockSet() == false) {
            notificationRendezvous.sendNotification(this);
        }
    }

    // --------------------------

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof InStream) {
            InStream that = (InStream) obj;

            String thisStr = toString();
            String thatStr = that.toString();

            return thisStr.equals(thatStr);
        }

        return false;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public String toString() {
        return str;
    }
}
