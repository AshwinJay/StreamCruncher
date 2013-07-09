package streamcruncher.test.func.generic;

import java.sql.Timestamp;
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
 * </p>
 * <p>
 * This test uses only one Stream. The other Streams are dormant.
 * </p>
 */

public abstract class PreFilterSyntaxTest extends MultiStreamEventGenerator {
    /**
     * Timestamp column will be filled later.
     */
    protected final Event[] eventSequence = new Event[] {
            new Event(EventType.stg1_event, new Object[] { "YHOO", 28.0, null, 1L }, 3),
            new Event(EventType.pause, new Object[] { 1000L }, -1),
            new Event(EventType.stg1_event, new Object[] { "GOOG", 27.0, null, 2L }, 3),
            new Event(EventType.pause, new Object[] { 1000L }, -1),
            new Event(EventType.stg1_event, new Object[] { "GOOG", 25.0, null, 3L }, 3),
            new Event(EventType.pause, new Object[] { 1000L }, -1),
            new Event(EventType.stg1_event, new Object[] { "YHOO", 30.0, null, 4L }, 3),
            new Event(EventType.pause, new Object[] { 1000L }, -1),
            new Event(EventType.stg1_event, new Object[] { "GOOG", 30.5, null, 5L }, 3),
            new Event(EventType.pause, new Object[] { 1000L }, -1),
            new Event(EventType.stg1_event, new Object[] { "YHOO", 29.75, null, 6L }, 3),
            new Event(EventType.pause, new Object[] { 1000L }, -1),
            new Event(EventType.stg1_event, new Object[] { "YHOO", 26.0, null, 7L }, 3),
            new Event(EventType.pause, new Object[] { 1000L }, -1),
            new Event(EventType.stg1_event, new Object[] { "GOOG", 26.0, null, 8L }, 3),
            new Event(EventType.pause, new Object[] { 1000L }, -1),
            new Event(EventType.stg1_event, new Object[] { "GOOG", 27.0, null, 9L }, 3),
            new Event(EventType.pause, new Object[] { 1000L }, -1),
            new Event(EventType.stg1_event, new Object[] { "YHOO", 25.5, null, 10L }, 3),
            new Event(EventType.pause, new Object[] { 1000L }, -1),
            new Event(EventType.stg1_event, new Object[] { "YHOO", 22.0, null, 11L }, 3),
            new Event(EventType.pause, new Object[] { 1000L }, -1),
            new Event(EventType.stg1_event, new Object[] { "GOOG", 22.5, null, 12L }, 3),
            new Event(EventType.pause, new Object[] { 1000L }, -1),
            new Event(EventType.stg1_event, new Object[] { "YHOO", 24.5, null, 13L }, 3),
            new Event(EventType.pause, new Object[] { 1000L }, -1),
            new Event(EventType.stg1_event, new Object[] { "YHOO", 26.5, null, 14L }, 3) };

    @Override
    protected String[] getResultColumnTypes() {
        return new String[] { RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 15),
                java.lang.Double.class.getName(), java.sql.Timestamp.class.getName(),
                java.lang.Long.class.getName() };
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
        return new String[] { "levela", "levelb", "event_time", "event_id" };
    }

    @Override
    protected String getRQL() {
        return "select levela, levelb, event_time, event_id"
                + " from stg1_event (partition store latest 10 where levela in ('YHOO') and levelb > 27.0) as yahooQuotes"
                + " where yahooQuotes.$row_status is new;";
    }

    @Override
    protected Event[] createEventArray() {
        return eventSequence;
    }

    @Override
    protected void verify(List<BatchResult> results) {
        int rowCount = 0;

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
                 * "levela", "levelb".
                 */
                String levela = (String) objects[0];
                Double levelb = (Double) objects[1];

                Assert.assertEquals(levela, "YHOO", "Actual and Expected results don't match");
                Assert.assertTrue((levelb.doubleValue() > 27.0),
                        "Actual and Expected results don't match");

                rowCount++;

                System.out.println();
            }
        }

        Assert.assertFalse((rowCount <= 0), "Received no Results");
    }
}