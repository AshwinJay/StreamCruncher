package streamcruncher.test.func.generic;

import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.List;

import org.testng.Assert;

import streamcruncher.api.artifact.RowSpec;
import streamcruncher.api.artifact.RowSpec.Info;
import streamcruncher.test.func.BatchResult;
import streamcruncher.test.func.MultiStreamEventGenerator;

/*
 * Author: Ashwin Jayaprakash Date: Mar 14, 2007 Time: 8:10:02 PM
 */

/**
 * <p>
 * This test uses only one Stream. The other Streams are dormant.
 * </p>
 */

public abstract class OutOfOrderEventTest extends MultiStreamEventGenerator {
    protected static final int windowSeconds = 5;

    /**
     * Timestamp column will be filled later.
     */
    protected final Event[] eventSequence = new Event[] {
            new Event(EventType.stg1_event, new Object[] { "YHOO", 28.0,
                    new Timestamp(System.currentTimeMillis()), 1L }, 3),
            new Event(EventType.stg1_event, new Object[] { "GOOG", 27.0,
                    new Timestamp(System.currentTimeMillis()), 2L }, 3),
            new Event(EventType.stg1_event, new Object[] { "GOOG", 25.0,
                    new Timestamp(System.currentTimeMillis()), 3L }, 3),
            new Event(EventType.stg1_event, new Object[] { "YHOO", 30.0,
                    new Timestamp(System.currentTimeMillis()), 4L }, 3),
            new Event(EventType.stg1_event, new Object[] { "YHOO", 26.0,
                    new Timestamp(System.currentTimeMillis()), 7L }, 3),

            new Event(EventType.pause, new Object[] { 5500L }, -1),
            new Event(EventType.stg1_event, new Object[] { "GOOG", 26.0,
                    new Timestamp(System.currentTimeMillis() + 5500), 8L }, 3),
            // ---Out of order events.
            new Event(EventType.stg1_event, new Object[] { "GOOG", 30.5,
                    new Timestamp(System.currentTimeMillis() + 1000), 5L }, 3),
            new Event(EventType.stg1_event, new Object[] { "YHOO", 29.75,
                    new Timestamp(System.currentTimeMillis() + 1000), 6L }, 3),
            // ---
            new Event(EventType.stg1_event, new Object[] { "GOOG", 27.0,
                    new Timestamp(System.currentTimeMillis() + 5500), 9L }, 3),
            new Event(EventType.stg1_event, new Object[] { "YHOO", 25.5,
                    new Timestamp(System.currentTimeMillis() + 5500), 10L }, 3),

            new Event(EventType.pause, new Object[] { 1500L }, -1),
            new Event(EventType.stg1_event, new Object[] { "YHOO", 22.0,
                    new Timestamp(System.currentTimeMillis() + 6500), 11L }, 3),
            new Event(EventType.stg1_event, new Object[] { "GOOG", 22.5,
                    new Timestamp(System.currentTimeMillis() + 6500), 12L }, 3),
            new Event(EventType.stg1_event, new Object[] { "YHOO", 24.5,
                    new Timestamp(System.currentTimeMillis() + 6500), 13L }, 3),

            new Event(EventType.pause, new Object[] { 3500L }, -1),
            new Event(EventType.stg1_event, new Object[] { "YHOO", 26.5,
                    new Timestamp(System.currentTimeMillis() + 9500), 14L }, 3) };

    protected String getCurrentTimestampKeyword() {
        return "current_timestamp";
    }

    @Override
    protected String[] getResultColumnTypes() {
        return new String[] { RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 15),
                java.lang.Double.class.getName(), java.sql.Timestamp.class.getName(),
                java.lang.Long.class.getName(), java.sql.Timestamp.class.getName() };
    }

    @Override
    protected String[] getStage1ColumnTypes() {
        return new String[] { RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 15),
                java.lang.Double.class.getName(), java.sql.Timestamp.class.getName(),
                java.lang.Long.class.getName() };
    }

    @Override
    protected String[] getStage2ColumnTypes() {
        return new String[] { RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 15),
                java.lang.Double.class.getName(), java.sql.Timestamp.class.getName(),
                java.lang.Long.class.getName() };
    }

    @Override
    protected String[] getStage3ColumnTypes() {
        return new String[] { RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 15),
                java.lang.Double.class.getName(), java.sql.Timestamp.class.getName(),
                java.lang.Long.class.getName() };
    }

    @Override
    protected String[] getResultColumnNames() {
        return new String[] { "levela", "levelb", "event_time", "event_id",
                getCurrentTimestampKeyword() };
    }

    @Override
    protected String getRQL() {
        return "select levela, levelb, event_time, event_id, " + getCurrentTimestampKeyword()
                + " from stg1_event (partition store last " + windowSeconds + " seconds max "
                + windowSeconds + ") as quotes"
                + " where quotes.$row_status is not dead order by event_id;";
    }

    @Override
    protected Event[] createEventArray() {
        return eventSequence;
    }

    @Override
    protected void verify(List<BatchResult> results) {
        long[] idOrder = { 1, 2, 3, 4, 7, 5, 6, 8, 9, 10, 11, 12, 13, 14 };
        LinkedHashMap<Long, Long> ids = new LinkedHashMap<Long, Long>();

        System.out.println("--Results--");
        for (BatchResult result : results) {
            System.out.println("Batch created at: " + new Timestamp(result.getTimestamp())
                    + ". Rows: " + result.getRows().size());

            List<Object[]> rows = result.getRows();

            System.out.println(" Batch results");
            for (Object[] objects : rows) {
                System.out.print(" ");
                for (Object object : objects) {
                    System.out.print(object + " ");
                }

                /*
                 * "levela", "levelb", "event_time", "event_id", "curr_time".
                 */
                String levela = (String) objects[0];
                Double levelb = (Double) objects[1];
                long eventId = (Long) objects[3];
                Timestamp inputTime = (Timestamp) objects[2];
                Timestamp outputTime = (Timestamp) objects[4];

                long diff = outputTime.getTime() - inputTime.getTime();
                ids.put(eventId, diff);

                System.out.println();
            }
        }

        System.out.println("Received Ids: " + ids);

        int time = (windowSeconds * 1000) +
        /*
         * Extra time, just in case; because the original timestamps were
         * hardcoded.
         */
        1000;

        for (int i = 0; i < idOrder.length; i++) {
            Assert.assertTrue(ids.containsKey(idOrder[i]), "Results missing");

            Assert.assertFalse(ids.get(idOrder[i]) > time,
                    "Event was allowed to stay for longer than " + time + " seconds");
        }

        int i = 0;
        for (Long l : ids.keySet()) {

            i++;
        }
    }
}