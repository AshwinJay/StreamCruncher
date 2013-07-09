package streamcruncher.test.func.generic;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import streamcruncher.api.CustomStore;
import streamcruncher.api.artifact.RowSpec;
import streamcruncher.api.artifact.RowSpec.Info;
import streamcruncher.test.func.BatchResult;
import streamcruncher.test.func.MultiStreamEventGenerator;

/*
 * Author: Ashwin Jayaprakash Date: Mar 14, 2007 Time: 8:10:02 PM
 */

/**
 * Special feature. Not for everyday use.
 */
public abstract class StockPriceComparison2Test extends MultiStreamEventGenerator {
    @Override
    protected void init() throws Exception {
        System.setProperty("custom.storename", CustomStoreImpl.class.getName());

        super.init();
    }

    /**
     * Timestamp column will be filled later.
     */
    protected final Event[] eventSequence = new Event[] {
            new Event(EventType.stg1_event, new Object[] { "YHOO", 28.0, null, 1L }, 3),
            new Event(EventType.pause, new Object[] { 1000L }, -1),
            new Event(EventType.stg1_event, new Object[] { "YHOO", 27.0, null, 2L }, 3),
            new Event(EventType.pause, new Object[] { 1000L }, -1),
            new Event(EventType.stg1_event, new Object[] { "GOOG", 25.0, null, 3L }, 3),
            new Event(EventType.pause, new Object[] { 1000L }, -1),
            new Event(EventType.stg1_event, new Object[] { "YHOO", 30.0, null, 4L }, 3),
            new Event(EventType.pause, new Object[] { 1000L }, -1),
            new Event(EventType.stg1_event, new Object[] { "GOOG", 30.5, null, 5L }, 3),
            new Event(EventType.pause, new Object[] { 1000L }, -1),
            new Event(EventType.stg1_event, new Object[] { "GOOG", 29.75, null, 6L }, 3),
            new Event(EventType.pause, new Object[] { 1000L }, -1),
            new Event(EventType.stg1_event, new Object[] { "GOOG", 26.0, null, 7L }, 3),
            new Event(EventType.pause, new Object[] { 1000L }, -1),
            new Event(EventType.stg1_event, new Object[] { "YHOO", 26.0, null, 8L }, 3),
            new Event(EventType.pause, new Object[] { 1000L }, -1),
            new Event(EventType.stg1_event, new Object[] { "YHOO", 27.0, null, 9L }, 3),
            new Event(EventType.pause, new Object[] { 1000L }, -1),
            new Event(EventType.stg1_event, new Object[] { "GOOG", 25.5, null, 10L }, 3),
            new Event(EventType.pause, new Object[] { 1000L }, -1),
            new Event(EventType.stg1_event, new Object[] { "YHOO", 32.0, null, 11L }, 3),
            new Event(EventType.pause, new Object[] { 1000L }, -1),
            new Event(EventType.stg1_event, new Object[] { "GOOG", 22.5, null, 12L }, 3),
            new Event(EventType.pause, new Object[] { 1000L }, -1),
            new Event(EventType.stg1_event, new Object[] { "YHOO", 33.5, null, 13L }, 3),
            new Event(EventType.pause, new Object[] { 1000L }, -1),
            new Event(EventType.stg1_event, new Object[] { "YHOO", 26.5, null, 14L }, 3),

            new Event(EventType.stg1_event, new Object[] { "GOOG", 19.0, null, 15L }, 3),
            new Event(EventType.pause, new Object[] { 1000L }, -1),
            new Event(EventType.stg1_event, new Object[] { "YHOO", 20.0, null, 16L }, 3),
            new Event(EventType.pause, new Object[] { 1000L }, -1),
            new Event(EventType.stg1_event, new Object[] { "YHOO", 22.4, null, 17L }, 3),
            new Event(EventType.pause, new Object[] { 1000L }, -1),
            new Event(EventType.stg1_event, new Object[] { "GOOG", 21.5, null, 18L }, 3),
            new Event(EventType.pause, new Object[] { 1000L }, -1),
            new Event(EventType.stg1_event, new Object[] { "YHOO", 28.0, null, 19L }, 3),
            new Event(EventType.pause, new Object[] { 1000L }, -1),
            new Event(EventType.stg1_event, new Object[] { "GOOG", 29.5, null, 20L }, 3),
            new Event(EventType.pause, new Object[] { 1000L }, -1),
            new Event(EventType.stg1_event, new Object[] { "YHOO", 50.7, null, 21L }, 3),
            new Event(EventType.pause, new Object[] { 1000L }, -1),
            new Event(EventType.stg1_event, new Object[] { "GOOG", 590.1, null, 22L }, 3)

    };

    @Override
    protected String[] getResultColumnNames() {
        return new String[] { "avgPrice", "avgPrice" };
    }

    @Override
    protected String getRQL() {
        return "select avgY, avgG"
                + " from stg1_event (partition store last 5 with avg(levelb) as avgY where levela = 'YHOO') as yhoo,"
                + " stg1_event (partition store last 5 with avg(levelb) as avgG where levela = 'GOOG') as goog"
                + " where yhoo.avgY!= goog.avgG;";
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

                System.out.println();
            }
        }
    }

    public static class CustomStoreImpl implements CustomStore {
        public void init(String queryName, Map<String, RowSpec> sourceTblAliasAndRowSpec,
                String whereClause) {
        }

        public void startBatch() {
        }

        public void added(String alias, Long id, Object[] data) {
            System.err.println("Added: " + alias + ", " + Arrays.asList(data));
        }

        public void removed(String alias, Long id, Object[] data) {
            System.err.println("Removed: " + alias + ", " + Arrays.asList(data));
        }

        public List<Object[]> endBatch() {
            return new LinkedList<Object[]>();
        }

        public void destroy() {
        }
    }
}