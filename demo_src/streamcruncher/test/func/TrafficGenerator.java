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
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
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
 * Author: Ashwin Jayaprakash Date: Sep 10, 2006 Time: 4:18:54 PM
 */
/**
 * <p>
 * Test Case with a single Stream of Vehicular traffic data.
 * </p>
 * <p>
 * <b>Note:</b> Some of the "Time based Window" Tests may fail occassionally,
 * because the expiry times of Events and hence the expected results are
 * dependent on the Hardware's performance. A slight delay in scheduling the
 * Query might cause the Test case to fail. In such cases, try running the Test
 * again or correct the expected behaviour.
 * </p>
 */
public abstract class TrafficGenerator {
    protected StreamCruncher cruncher;

    protected SortedMap<Long, Object[]> generatedEvents;

    protected TrafficGenerator() {
        cruncher = new StreamCruncher();
        generatedEvents = new TreeMap<Long, Object[]>();
    }

    protected void init() throws Exception {
        String[] columnNames = { "event_id", "event_time", "vehicle_id", "seg", "speed" };
        String[] columnTypes = getColumnTypes();
        RowSpec rowSpec = new RowSpec(columnNames, columnTypes, 0, 1);
        cruncher.registerInStream("test", rowSpec, getInStreamBlockSize());

        // ---------

        String rql = getRQL();

        String queryName = "test_res_rql";

        ParserParameters parameters = new ParserParameters();
        parameters.setQuery(rql);
        parameters.setQueryName(queryName);
        columnTypes = getResultColumnTypes();
        parameters.setResultColumnTypes(columnTypes);

        ParsedQuery parsedQuery = cruncher.parseQuery(parameters);

        QueryConfig config = parsedQuery.getQueryConfig();
        config.setQuerySchedulePolicy(new QueryConfig.QuerySchedulePolicyValue(
                QuerySchedulePolicy.ATLEAST_OR_SOONER, Integer.MAX_VALUE));

        modifyQueryConfig(config);

        cruncher.registerQuery(parsedQuery);
    }

    protected int getInStreamBlockSize() {
        return 1024;
    }

    protected void modifyQueryConfig(QueryConfig config) {
    }

    protected abstract String[] getColumnTypes();

    protected abstract String getRQL();

    protected String getCurrentTimestampKeyword() {
        return "current_timestamp";
    }

    protected abstract String[] getResultColumnTypes();

    protected int getMaxDataRows() {
        return 12;
    }

    protected Iterator<Object[]> getData() {
        Iterator<Object[]> iter = new Iterator<Object[]>() {
            private final Random random = new Random();

            private int counter = 1;

            public boolean hasNext() {
                return counter <= getMaxDataRows();
            }

            public Object[] next() {
                // "event_id", "event_time", "vehicle_id", "seg", "speed"
                Object[] data = new Object[5];

                data[0] = new Long(counter);
                data[1] = new Timestamp(TrafficGenerator.this.getEventTimeStamp(counter));
                data[2] = "veh-" + counter;
                data[3] = new Integer(random.nextInt(3));
                data[4] = new Double(random.nextInt(50));

                System.out.println(Arrays.asList(data));

                counter++;

                generatedEvents.put((Long) data[0], data);

                return data;
            }

            /**
             * @throws UnsupportedOperationException
             */
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };

        return iter;
    }

    protected List<BatchResult> test() throws Exception {
        InputSession inputSession = cruncher.createInputSession("test");
        inputSession.start();

        generateEvents(inputSession);

        // --------------

        OutputSession outputSession = cruncher.createOutputSession("test_res_rql");
        List<BatchResult> results = fetchResults(outputSession);

        verify(results);

        return results;
    }

    protected void generateEvents(InputSession inputSession) throws StreamCruncherException {
        Iterator<Object[]> iter = getData();
        for (int counter = 0; iter.hasNext();) {
            Object[] data = iter.next();

            inputSession.submitEvent(data);

            counter++;
            afterEvent(counter);
        }

        inputSession.close();
    }

    protected long getEventTimeStamp(int counter) {
        return System.currentTimeMillis();
    }

    protected void afterEvent(int counter) {
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
            cruncher.unregisterQuery("test_res_rql");
        }
        catch (RuntimeException e) {
            e.printStackTrace(System.err);
        }

        try {
            cruncher.unregisterInStream("test");
        }
        catch (StreamCruncherException e) {
            e.printStackTrace(System.err);
        }

        cruncher = null;
    }
}
