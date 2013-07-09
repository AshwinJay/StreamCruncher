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
package streamcruncher.test.func;

import java.sql.Timestamp;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import streamcruncher.api.InputSession;
import streamcruncher.api.OutputSession;
import streamcruncher.api.ParsedQuery;
import streamcruncher.api.ParserParameters;
import streamcruncher.api.QueryConfig;
import streamcruncher.api.StreamCruncher;
import streamcruncher.api.StreamCruncherException;
import streamcruncher.api.QueryConfig.QuerySchedulePolicy;
import streamcruncher.api.artifact.RowSpec;

/*
 * Author: Ashwin Jayaprakash Date: Oct 15, 2006 Time: 11:35:36 AM
 */

/**
 * Generates test data for 3 Streams - for Correlation tests.
 */
public abstract class MultiStreamEventGenerator {
    protected StreamCruncher cruncher;

    protected LinkedBlockingQueue<Long> eventIds;

    protected EnumMap<MultiStreamEventGenerator.EventType, Cache> cacheMap;

    protected MultiStreamEventGenerator() {
        cruncher = new StreamCruncher();
    }

    protected void init() throws Exception {
        String[] columnNames = { "levela", "levelb", "event_time", "event_id" };
        String[] columnTypes = getStage1ColumnTypes();
        RowSpec rowSpec = new RowSpec(columnNames, columnTypes, 3, 2);
        cruncher.registerInStream(EventType.stg1_event.name(), rowSpec, 4096);

        // ---------

        columnNames = new String[] { "levela", "levelb", "event_time", "event_id" };
        columnTypes = getStage2ColumnTypes();
        rowSpec = new RowSpec(columnNames, columnTypes, 3, 2);
        cruncher.registerInStream(EventType.stg2_event.name(), rowSpec, 4096);

        // ---------

        columnNames = new String[] { "levela", "levelb", "event_time", "event_id" };
        columnTypes = getStage3ColumnTypes();
        rowSpec = new RowSpec(columnNames, columnTypes, 3, 2);
        cruncher.registerInStream(EventType.stg3_event.name(), rowSpec, 4096);

        // ---------

        beforeQueryParse();

        eventIds = new LinkedBlockingQueue<Long>();
        cacheMap = new EnumMap<EventType, Cache>(EventType.class);

        // ---------

        String rql = getRQL();

        columnNames = getResultColumnNames();
        columnTypes = getResultColumnTypes();

        String queryName = "event_correlation_rql";

        ParserParameters parameters = new ParserParameters();
        parameters.setQuery(rql);
        parameters.setQueryName(queryName);
        parameters.setResultColumnTypes(columnTypes);

        ParsedQuery parsedQuery = cruncher.parseQuery(parameters);

        QueryConfig config = parsedQuery.getQueryConfig();
        config.setQuerySchedulePolicy(new QueryConfig.QuerySchedulePolicyValue(
                QuerySchedulePolicy.ATLEAST_OR_SOONER, Integer.MAX_VALUE));

        modifyQueryConfig(config);

        cruncher.registerQuery(parsedQuery);
    }

    protected void beforeQueryParse() {
    }

    protected void modifyQueryConfig(QueryConfig config) {
    }

    protected abstract String[] getStage1ColumnTypes();

    protected abstract String[] getStage2ColumnTypes();

    protected abstract String[] getStage3ColumnTypes();

    protected abstract String getRQL();

    protected abstract String[] getResultColumnNames();

    protected abstract String[] getResultColumnTypes();

    protected String getRQLColumnsCSV() {
        String[] columns = getResultColumnNames();
        String csv = "";
        for (int i = 0; i < columns.length; i++) {
            if (csv != "") {
                csv = csv + ", ";
            }
            csv = csv + columns[i];
        }

        return csv;
    }

    protected abstract Event[] createEventArray();

    protected Iterator<Event> getEvents() {
        final Event[] eventSequence = createEventArray();

        Iterator<Event> iter = new Iterator<Event>() {
            int counter = 0;

            public boolean hasNext() {
                return !(counter == eventSequence.length);
            }

            public Event next() {
                Event retVal = eventSequence[counter++];

                Object[] data = retVal.getData();
                for (int i = 0; i < data.length; i++) {
                    if (data[i] == null) {
                        data[i] = new Timestamp(System.currentTimeMillis());
                    }
                }

                for (Object object : data) {
                    System.out.print(object + " ");
                }
                System.out.println();

                return retVal;
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }
        };

        return iter;
    }

    protected List<BatchResult> test() throws Exception {
        InputSession inputSession1 = cruncher.createInputSession(EventType.stg1_event.name());
        inputSession1.start();
        cacheMap.put(EventType.stg1_event, new Cache(inputSession1));

        InputSession inputSession2 = cruncher.createInputSession(EventType.stg2_event.name());
        inputSession2.start();
        cacheMap.put(EventType.stg2_event, new Cache(inputSession2));

        InputSession inputSession3 = cruncher.createInputSession(EventType.stg3_event.name());
        inputSession3.start();
        cacheMap.put(EventType.stg3_event, new Cache(inputSession3));

        OutputSession outputSession = cruncher.createOutputSession("event_correlation_rql");

        // --------------

        Iterator<Event> events = getEvents();
        for (; events.hasNext();) {
            Event event = events.next();
            handleEvent(event);
        }

        // --------------

        List<BatchResult> results = fetchResults(outputSession);
        verify(results);

        Cache cache = cacheMap.get(EventType.stg1_event);
        cache.inputSession.close();

        cache = cacheMap.get(EventType.stg2_event);
        cache.inputSession.close();

        cache = cacheMap.get(EventType.stg3_event);
        cache.inputSession.close();

        outputSession.close();

        return results;
    }

    protected void handleEvent(Event event) throws InterruptedException {
        EventType type = event.type;
        Cache cache = cacheMap.get(type);

        if (type == EventType.pause) {
            Long time = (Long) event.data[0];
            Thread.sleep(time);
        }
        else {
            cache.inputSession.submitEvent(event.data);
        }
    }

    protected List<BatchResult> fetchResults(OutputSession outputSession) throws Exception {
        outputSession.start();
        int pollTimeSec = 10;
        List<BatchResult> results = new LinkedList<BatchResult>();
        while (true) {
            try {
                List<Object[]> events = outputSession.readEvents(pollTimeSec, TimeUnit.SECONDS);
                if (events.size() == 0) {
                    if (results.size() > 0) {
                        throw new InterruptedException("Timed out!");
                    }

                    System.err.println("Retrying..");
                    continue;
                }

                BatchResult batchResult = new BatchResult();
                for (Object[] objects : events) {
                    batchResult.addRow(objects);
                }

                results.add(batchResult);
                System.err.println("Got result batch.");
            }
            catch (InterruptedException e) {
                if (pollTimeSec == 10) {
                    pollTimeSec = 5;
                }
                else if (pollTimeSec > 1) {
                    pollTimeSec--;
                }
                else {
                    break;
                }

                System.err.println("Retrying for more results...");
            }
        }

        outputSession.close();

        return results;
    }

    protected abstract void verify(List<BatchResult> results);

    protected void discard() {
        try {
            cruncher.unregisterQuery("event_correlation_rql");
        }
        catch (RuntimeException e) {
            e.printStackTrace(System.err);
        }

        try {
            cruncher.unregisterInStream(EventType.stg1_event.name());
        }
        catch (StreamCruncherException e) {
            e.printStackTrace(System.err);
        }

        try {
            cruncher.unregisterInStream(EventType.stg2_event.name());
        }
        catch (StreamCruncherException e) {
            e.printStackTrace(System.err);
        }

        try {
            cruncher.unregisterInStream(EventType.stg3_event.name());
        }
        catch (StreamCruncherException e) {
            e.printStackTrace(System.err);
        }

        cruncher = null;
    }

    // ---------------

    public static enum EventType {
        stg1_event, stg2_event, stg3_event, pause;
    }

    public static class Event {
        protected final EventType type;

        protected Object[] data;

        protected final int idPosition;

        public Event(EventType type, Object[] data, int idPosition) {
            this.type = type;
            this.data = data;
            this.idPosition = idPosition;
        }

        public Object[] getData() {
            return data;
        }

        public void setData(Object[] data) {
            this.data = data;
        }

        public EventType getType() {
            return type;
        }

        public int getIdPosition() {
            return idPosition;
        }
    }

    protected static class Cache {
        protected final InputSession inputSession;

        public Cache(InputSession inputSession) {
            this.inputSession = inputSession;
        }
    }
}
