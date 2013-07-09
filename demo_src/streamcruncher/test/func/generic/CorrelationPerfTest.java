package streamcruncher.test.func.generic;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
import streamcruncher.test.func.BatchResult;

/*
 * Author: Ashwin Jayaprakash Date: Oct 15, 2006 Time: 11:35:36 AM
 */

/**
 * <p>
 * Generates test data for 4 Streams - for Correlation tests. Three Queries scan
 * the Streams and perform Correlations.
 * </p>
 * <p>
 * {@link #test()} and {@link #pumpEvents()} methods have to be modified to run
 * and measure the performance.
 * </p>
 */
public abstract class CorrelationPerfTest {
    protected static enum Stream {
        TypeA, TypeB, TypeC, TypeD;
    }

    protected static final String queryNamePrefix = "rql";

    protected StreamCruncher cruncher;

    protected InputSession streamA;

    protected InputSession streamB;

    protected InputSession streamC;

    protected InputSession streamD;

    protected Map<Integer, QueryDetails> queryDetailsMap;

    protected CorrelationPerfTest() {
        cruncher = new StreamCruncher();
    }

    protected void init() throws Exception {
        String[][] streamColumnTypes = getStreamColumnTypes();

        String[] columnNames = { "event_id", "event_time" };
        String[] columnTypes = streamColumnTypes[0];
        RowSpec rowSpec = new RowSpec(columnNames, columnTypes, 0, 1);
        cruncher.registerInStream(Stream.TypeA.name(), rowSpec, 4096);

        // ---------

        columnNames = new String[] { "event_id", "event_time" };
        columnTypes = streamColumnTypes[1];
        rowSpec = new RowSpec(columnNames, columnTypes, 0, 1);
        cruncher.registerInStream(Stream.TypeB.name(), rowSpec, 4096);

        // ---------

        columnNames = new String[] { "event_id", "event_time" };
        columnTypes = streamColumnTypes[2];
        rowSpec = new RowSpec(columnNames, columnTypes, 0, 1);
        cruncher.registerInStream(Stream.TypeC.name(), rowSpec, 4096);

        // ---------

        columnNames = new String[] { "event_id", "event_time" };
        columnTypes = streamColumnTypes[3];
        rowSpec = new RowSpec(columnNames, columnTypes, 0, 1);
        cruncher.registerInStream(Stream.TypeD.name(), rowSpec, 4096);

        // ---------

        String[] rqls = getRQLs();

        queryDetailsMap = new HashMap<Integer, QueryDetails>();
        QueryDetails details = new QueryDetails();
        details.query = rqls[0];
        details.eventStreamsCSV = "A, B, C";
        queryDetailsMap.put(0, details);

        details = new QueryDetails();
        details.query = rqls[1];
        details.eventStreamsCSV = "C, D";
        queryDetailsMap.put(1, details);

        details = new QueryDetails();
        details.query = rqls[2];
        details.eventStreamsCSV = "A, C, D";
        queryDetailsMap.put(2, details);

        String[][] resultColumnTypes = getResultColumnTypes();
        for (int i = 0; i < rqls.length; i++) {
            columnTypes = resultColumnTypes[i];
            String queryName = queryNamePrefix + "_" + i;

            ParserParameters parameters = new ParserParameters();
            parameters.setQuery(rqls[i]);
            parameters.setQueryName(queryName);
            parameters.setResultColumnTypes(columnTypes);

            ParsedQuery parsedQuery = cruncher.parseQuery(parameters);

            QueryConfig config = parsedQuery.getQueryConfig();
            config.setQuerySchedulePolicy(new QueryConfig.QuerySchedulePolicyValue(
                    QuerySchedulePolicy.ATLEAST_OR_SOONER, Integer.MAX_VALUE));

            cruncher.registerQuery(parsedQuery);
        }
    }

    protected String[][] getResultColumnTypes() {
        String[] qry1 = { java.lang.Long.class.getName(), java.lang.Long.class.getName(),
                java.lang.Long.class.getName(), java.sql.Timestamp.class.getName(),
                java.sql.Timestamp.class.getName(), java.sql.Timestamp.class.getName(),
                java.sql.Timestamp.class.getName(), java.sql.Timestamp.class.getName() };
        String[] qry2 = { java.lang.Long.class.getName(), java.lang.Long.class.getName(),
                java.sql.Timestamp.class.getName(), java.sql.Timestamp.class.getName(),
                java.sql.Timestamp.class.getName() };
        String[] qry3 = { java.lang.Long.class.getName(), java.lang.Long.class.getName(),
                java.lang.Long.class.getName(), java.sql.Timestamp.class.getName(),
                java.sql.Timestamp.class.getName(), java.sql.Timestamp.class.getName(),
                java.sql.Timestamp.class.getName(), };
        return new String[][] { qry1, qry2, qry3 };
    }

    protected String[][] getStreamColumnTypes() {
        return new String[][] {
                { java.lang.Long.class.getName(), java.sql.Timestamp.class.getName() },
                { java.lang.Long.class.getName(), java.sql.Timestamp.class.getName() },
                { java.lang.Long.class.getName(), java.sql.Timestamp.class.getName() },
                { java.lang.Long.class.getName(), java.sql.Timestamp.class.getName() } };
    }

    protected String[] getRQLs() {
        // Select A-B-C triplet such that C occurs after A and B.
        String query1 = "select idA, idB, idC, timeA, timeB, timeC, current_timestamp as timeEnd from"
                //
                + " alert streamA.event_id as idA, streamA.event_time as timeA, streamB.event_id as idB,"
                + " streamB.event_time as timeB, streamC.event_id as idC, streamC.event_time as timeC"
                //
                + " using "
                + Stream.TypeA.name()
                + "(partition store last 10 seconds) as streamA correlate on event_id,"
                + Stream.TypeB.name()
                + "(partition store last 10 seconds) as streamB correlate on event_id,"
                + Stream.TypeC.name()
                + "(partition store last 10 seconds) as streamC correlate on event_id"
                //
                + " when present(streamA and streamB and streamC)"
                //
                + " where (timeB < timeC and timeA < timeC);";

        // Select C-D pair such that D occurs after C.
        String query2 = "select idC, idD, timeC, timeD, current_timestamp as timeEnd from"
                //
                + " alert streamC.event_id as idC, streamC.event_time as timeC,"
                + " streamD.event_id as idD, streamD.event_time as timeD"
                //
                + " using " + Stream.TypeC.name()
                + "(partition store last 10 seconds) as streamC correlate on event_id,"
                + Stream.TypeD.name()
                + "(partition store last 10 seconds) as streamD correlate on event_id"
                //
                + " when present(streamC and streamD)"
                //
                + " where (timeC < timeD);";

        // Select triplet A-C-D.
        String query3 = "select idA, idC, idD, timeA, timeC, timeD, current_timestamp as timeEnd from"
                //
                + " alert streamA.event_id as idA, streamA.event_time as timeA,"
                + " streamC.event_id as idC, streamC.event_time as timeC,"
                + " streamD.event_id as idD, streamD.event_time as timeD"
                //
                + " using "
                + Stream.TypeA.name()
                + "(partition store last 10 seconds) as streamA correlate on event_id,"
                + Stream.TypeC.name()
                + "(partition store last 10 seconds) as streamC correlate on event_id,"
                + Stream.TypeD.name()
                + "(partition store last 10 seconds) as streamD correlate on event_id"
                //
                + " when present(streamA and streamC and streamD);";

        return new String[] { query1, query2, query3 };
    }

    protected void test() throws Exception {
        streamA = cruncher.createInputSession(Stream.TypeA.name());
        streamA.start();

        streamB = cruncher.createInputSession(Stream.TypeB.name());
        streamB.start();

        streamC = cruncher.createInputSession(Stream.TypeC.name());
        streamC.start();

        streamD = cruncher.createInputSession(Stream.TypeD.name());
        streamD.start();

        for (int i = 0; i < 15; i++) {
            // Allow JVM warmup. No use capturing results during warmup.
            System.err.println("Running Test round: " + (i + 1));
            repeatTest(i >= 12);
        }

        streamA.close();
        streamB.close();
        streamC.close();
        streamD.close();
    }

    protected void repeatTest(boolean verify) throws StreamCruncherException, Exception {
        System.out.println();
        System.out.println("=======================================");
        System.out.println("Pumping Events...");

        TestRunDetails runDetails = pumpEvents();

        // --------------

        String[] rqls = getRQLs();

        for (int i = 0; i < rqls.length; i++) {
            String queryName = queryNamePrefix + "_" + i;

            OutputSession outputSession = cruncher.createOutputSession(queryName);

            List<BatchResult> results = fetchResults(rqls[i], outputSession);
            if (verify) {
                verify(i, runDetails, results);
            }

            System.out.println();

            outputSession.close();
        }
    }

    protected TestRunDetails pumpEvents() {
        TestRunDetails details = new TestRunDetails();

        for (long i = 0; i < 15000; i++) {
            // Skip these events every 50th time.
            if (i == 0 || i % 50 != 0) {
                Object[] event = new Object[] { i, new Timestamp(System.currentTimeMillis()) };
                streamA.submitEvent(event);
                details.totalPublishedTypeA++;

                event = new Object[] { i, new Timestamp(System.currentTimeMillis()) };
                streamB.submitEvent(event);
                details.totalPublishedTypeB++;

                // Add artificial delay.
                event = new Object[] { i, new Timestamp(System.currentTimeMillis() + 1) };
                streamC.submitEvent(event);
                details.totalPublishedTypeC++;
            }

            // Add artificial delay.
            Object[] event = new Object[] { i, new Timestamp(System.currentTimeMillis() + 2) };
            streamD.submitEvent(event);
            details.totalPublishedTypeD++;
        }

        return details;
    }

    protected List<BatchResult> fetchResults(String query, OutputSession outputSession)
            throws Exception {
        outputSession.start();
        int pollTimeSec = 5;

        /*
         * Wait for a while and let all the Streams complete their processing.
         * Otherwise, on a single CPU machine, the Result fetch operation will
         * steal CPU cycles from the Kernel processing Threads and affect
         * performance.
         */
        Thread.sleep(7000);
        System.out.println();
        System.out.println("---------------------------------------");
        System.out.println("Retrieving.." + query);

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
                if (pollTimeSec == 5) {
                    pollTimeSec = 3;
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

    /**
     * @param queryNumber
     * @param results
     */
    protected void verify(int queryNumber, TestRunDetails runDetails, List<BatchResult> results) {
        int totalEvents = 0;
        Long firstEventStart = null;
        Long lastEventEnd = null;

        for (BatchResult result : results) {
            List<Object[]> rows = result.getRows();
            for (Object[] row : rows) {
                for (Object object : row) {
                    if (firstEventStart == null && object instanceof Timestamp) {
                        firstEventStart = ((Timestamp) object).getTime();
                    }

                    System.out.print(object + " ");
                }

                totalEvents++;
                lastEventEnd = ((Timestamp) row[row.length - 1]).getTime();

                System.out.println();
            }
        }

        QueryDetails details = queryDetailsMap.get(queryNumber);

        System.out.println();
        System.out.println("---------------------------------------");
        System.out.println("Query: " + details.query);
        System.out.println("Input Streams: " + details.eventStreamsCSV);
        System.out.println("Events published by Stream A: " + runDetails.totalPublishedTypeA);
        System.out.println("Events published by Stream B: " + runDetails.totalPublishedTypeB);
        System.out.println("Events published by Stream C: " + runDetails.totalPublishedTypeC);
        System.out.println("Events published by Stream D: " + runDetails.totalPublishedTypeD);
        System.out
                .println("Total time taken (Last Result received - First Event sent) in seconds: "
                        + (lastEventEnd - firstEventStart) / 1000.0);
        System.out.println("Total Results published: " + totalEvents);
        System.out.println("---------------------------------------");
    }

    protected void discard() {
        String[] rqls = getRQLs();
        for (int i = 0; i < rqls.length; i++) {
            String queryName = queryNamePrefix + "_" + i;

            try {
                cruncher.unregisterQuery(queryName);
            }
            catch (RuntimeException e) {
                e.printStackTrace(System.err);
            }
        }

        try {
            cruncher.unregisterInStream(Stream.TypeA.name());
        }
        catch (StreamCruncherException e) {
            e.printStackTrace(System.err);
        }

        try {
            cruncher.unregisterInStream(Stream.TypeB.name());
        }
        catch (StreamCruncherException e) {
            e.printStackTrace(System.err);
        }

        try {
            cruncher.unregisterInStream(Stream.TypeC.name());
        }
        catch (StreamCruncherException e) {
            e.printStackTrace(System.err);
        }

        try {
            cruncher.unregisterInStream(Stream.TypeD.name());
        }
        catch (StreamCruncherException e) {
            e.printStackTrace(System.err);
        }

        streamA = null;
        streamB = null;
        streamC = null;
        streamD = null;
        cruncher = null;
    }

    // ----------

    protected static class TestRunDetails {
        protected int totalPublishedTypeA;

        protected int totalPublishedTypeB;

        protected int totalPublishedTypeC;

        protected int totalPublishedTypeD;
    }

    protected static class QueryDetails {
        protected String query;

        protected String eventStreamsCSV;

        protected int eventsPerStream;
    }
}
