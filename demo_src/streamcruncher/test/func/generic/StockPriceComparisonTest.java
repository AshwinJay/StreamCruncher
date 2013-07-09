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
 * This Test demonstrates the use of the <code>self#</code> keyword to perform
 * Self-Joins across a Partition. Self-Joins are very useful when we are trying
 * to look for a related Event inside the same Partition Window. Instead of
 * having to define multiple identical Partitions and then joining them, this
 * method lets the user create a single Partition first and then create
 * references to the same Partition using different aliases.
 * </p>
 * <p>
 * In this Test, a Sliding Window of size 3 is defined on a Stream of Stock
 * Quotes for a specific symbol. The intention is to retrieve Stock Price pairs
 * such that the latest price is atleast 2 Dollars higher than the previous 2
 * prices (still in the Window).
 * </p>
 * <p>
 * This test uses only one Stream. The other Streams are dormant.
 * </p>
 */

public abstract class StockPriceComparisonTest extends MultiStreamEventGenerator {
    /**
     * Timestamp column will be filled later.
     */
    protected final Event[] eventSequence = new Event[] {
            new Event(EventType.stg1_event, new Object[] { "YHOO", 28, null, 1L }, 3),
            new Event(EventType.pause, new Object[] { 1000L }, -1),
            new Event(EventType.stg1_event, new Object[] { "YHOO", 27, null, 2L }, 3),
            new Event(EventType.pause, new Object[] { 1000L }, -1),
            new Event(EventType.stg1_event, new Object[] { "YHOO", 25, null, 3L }, 3),
            new Event(EventType.pause, new Object[] { 1000L }, -1),
            new Event(EventType.stg1_event, new Object[] { "YHOO", 30, null, 4L }, 3),
            new Event(EventType.pause, new Object[] { 1000L }, -1),
            new Event(EventType.stg1_event, new Object[] { "YHOO", 30.5, null, 5L }, 3),
            new Event(EventType.pause, new Object[] { 1000L }, -1),
            new Event(EventType.stg1_event, new Object[] { "YHOO", 29.75, null, 6L }, 3),
            new Event(EventType.pause, new Object[] { 1000L }, -1),
            new Event(EventType.stg1_event, new Object[] { "YHOO", 26, null, 7L }, 3),
            new Event(EventType.pause, new Object[] { 1000L }, -1),
            new Event(EventType.stg1_event, new Object[] { "YHOO", 26, null, 8L }, 3),
            new Event(EventType.pause, new Object[] { 1000L }, -1),
            new Event(EventType.stg1_event, new Object[] { "YHOO", 27, null, 9L }, 3),
            new Event(EventType.pause, new Object[] { 1000L }, -1),
            new Event(EventType.stg1_event, new Object[] { "YHOO", 25.5, null, 10L }, 3),
            new Event(EventType.pause, new Object[] { 1000L }, -1),
            new Event(EventType.stg1_event, new Object[] { "YHOO", 22, null, 11L }, 3),
            new Event(EventType.pause, new Object[] { 1000L }, -1),
            new Event(EventType.stg1_event, new Object[] { "YHOO", 22.5, null, 12L }, 3),
            new Event(EventType.pause, new Object[] { 1000L }, -1),
            new Event(EventType.stg1_event, new Object[] { "YHOO", 24.5, null, 13L }, 3),
            new Event(EventType.pause, new Object[] { 1000L }, -1),
            new Event(EventType.stg1_event, new Object[] { "YHOO", 26.5, null, 14L }, 3) };

    @Override
    protected String[] getResultColumnNames() {
        return new String[] { "newPrice", "oldPrice" };
    }

    @Override
    protected String getRQL() {
        return "select cloneLastX.levelb as newPrice, lastXQuotes.levelb as oldPrice"
                + " from stg1_event (partition store last 3) as lastXQuotes,"
                + " self#lastXQuotes as cloneLastX"
                + " where cloneLastX.$row_status is new and lastXQuotes.$row_status is not dead"
                + " and (cloneLastX.levelb - 2.0 ) >= lastXQuotes.levelb order by oldPrice;";
    }

    @Override
    protected String[] getResultColumnTypes() {
        return new String[] { java.lang.Double.class.getName(), java.lang.Double.class.getName() };
    }

    @Override
    protected String[] getStage1ColumnTypes() {
        return new String[] { RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 10),
                java.lang.Double.class.getName(), java.sql.Timestamp.class.getName(),
                java.lang.Long.class.getName() };
    }

    @Override
    protected String[] getStage2ColumnTypes() {
        return new String[] { RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 10),
                java.lang.Double.class.getName(), java.sql.Timestamp.class.getName(),
                java.lang.Long.class.getName() };
    }

    @Override
    protected String[] getStage3ColumnTypes() {
        return new String[] { RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 10),
                java.lang.Double.class.getName(), java.sql.Timestamp.class.getName(),
                java.lang.Long.class.getName() };
    }

    @Override
    protected Event[] createEventArray() {
        return eventSequence;
    }

    @Override
    protected void verify(List<BatchResult> results) {
        String[] resultArray = { "30.0 25.0", "30.0 27.0", "30.5 25.0", "24.5 22.0", "24.5 22.5",
                "26.5 22.5", "26.5 24.5" };
        int i = 0;

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
                 * "newPrice", "oldPrice".
                 */
                Double levela = (Double) objects[0];
                Double levelb = (Double) objects[1];

                String hash = levela + " " + levelb;
                Assert
                        .assertEquals(hash, resultArray[i],
                                "Actual and Expected results don't match");
                i++;

                System.out.println();
            }
        }

        Assert.assertEquals(i, resultArray.length,
                "Actual results are less than expected number of results");
    }
}