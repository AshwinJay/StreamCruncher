package streamcruncher.test.func.generic;

import java.sql.Timestamp;
import java.util.List;

import org.testng.Assert;

import streamcruncher.api.StreamCruncherException;
import streamcruncher.api.aggregator.DiffBaselineProvider;
import streamcruncher.api.artifact.RowSpec;
import streamcruncher.api.artifact.RowSpec.Info;
import streamcruncher.test.func.BatchResult;
import streamcruncher.test.func.MultiStreamEventGenerator;

/*
 * Author: Ashwin Jayaprakash Date: Mar 14, 2007 Time: 8:10:02 PM
 */

/**
 * <p>
 * This Test demonstrates the use of the Extra parameters allowed in the
 * In-built Aggregation functions. The <code>$diff</code> parameter provided,
 * instructs the function to compute the difference between the New value and
 * the Old value, instead of outputting the New value directly had the extra
 * clause not been provided.
 * </p>
 * <p>
 * This example demonstrates a case where a Servers in a Datacenter keeps
 * pinging the ESP Kernel with Heartbeat Events every 5 seconds. With the use of
 * the <code>pinned</code> clause, the Partitions/Window remain in memory even
 * if the Window becomes empty - when the Server fails to ping within 5 seconds;
 * usually if the Server has gone down or the Network has partitioned. By using
 * the <code>count(xxcolumn $diff)</code> function definition, the Window and
 * hence the Aggregate remain in memory even if the Ping Event is not received
 * in time. The "count" dips to 0 and the "diff" (New value - Old value) turns
 * negative, which is when the Alarm is raised. The <code>avg(..)</code>
 * function can also be used to identify missed alerts when the avgrage value
 * becomes <code>null</code>.
 * </p>
 * There are 3 <code>count(.. )</code> functions defined for
 * <code>levelb</code>. One does not use the <code>$diff</code> clause and
 * so, it provides the absolute count of Events over the past 5 seconds. Two of
 * them use the <code>$diff</code> clause, out of which one uses a custom
 * {@link DiffBaselineProvider}, which always uses 0 as the baseline. The other
 * one, for which no custom Provider has been declared, uses the default
 * Provider.
 * <p>
 * <p>
 * Thus, Aggregates can be used to detect Trends in the Stream.
 * </p>
 * <p>
 * This test uses only one Stream. The other Streams are dormant.
 * </p>
 */

// todo Test Restart with Provider - Diff and WindowSize.
public abstract class ClusterHealthTest extends MultiStreamEventGenerator {
    /**
     * Timestamp column will be filled later.
     */
    protected final Event[] eventSequence = new Event[] {
            new Event(EventType.stg1_event, new Object[] { "datacntr1", "ultrasparc14", null, 1L },
                    3),
            new Event(EventType.pause, new Object[] { 5000L }, -1),
            new Event(EventType.stg1_event, new Object[] { "datacntr1", "ultrasparc14", null, 2L },
                    3),
            new Event(EventType.pause, new Object[] { 5000L }, -1),
            new Event(EventType.stg1_event, new Object[] { "datacntr1", "ultrasparc14", null, 3L },
                    3),
            // Long wait. Restart counting.
            new Event(EventType.pause, new Object[] { 10000L }, -1),
            new Event(EventType.stg1_event, new Object[] { "datacntr1", "ultrasparc14", null, 4L },
                    3),
            new Event(EventType.pause, new Object[] { 5000L }, -1),
            new Event(EventType.stg1_event, new Object[] { "datacntr1", "ultrasparc14", null, 5L },
                    3) };

    @Override
    protected String[] getResultColumnNames() {
        return new String[] { "levela", "levelb", "change", "change2", "beats", "testcol1",
                "testcol2", "testcol3" };
    }

    @Override
    protected String[] getResultColumnTypes() {
        return new String[] { RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 15),
                RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 15),
                java.lang.Integer.class.getName(), java.lang.Integer.class.getName(),
                java.lang.Integer.class.getName(), java.lang.Double.class.getName(),
                java.lang.Double.class.getName(), java.lang.Double.class.getName() };
    }

    @Override
    protected String[] getStage1ColumnTypes() {
        return new String[] { RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 15),
                RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 15),
                java.sql.Timestamp.class.getName(), java.lang.Long.class.getName() };
    }

    @Override
    protected String[] getStage2ColumnTypes() {
        return new String[] { RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 15),
                RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 15),
                java.sql.Timestamp.class.getName(), java.lang.Long.class.getName() };
    }

    @Override
    protected String[] getStage3ColumnTypes() {
        return new String[] { RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 15),
                RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 15),
                java.sql.Timestamp.class.getName(), java.lang.Long.class.getName() };
    }

    @Override
    protected String getRQL() {
        String csv = getRQLColumnsCSV();

        return "select " + csv + " from stg1_event (partition by levela, levelb"
                + " store last 5 seconds with pinned count(event_id $diff) as change,"
                + " count(event_id $diff 'DiffBaseline/CountBL') as change2,"
                + " count(event_id) as beats, avg(event_id $diff) as testcol1,"
                + " avg(event_id $diff 'DiffBaseline/AvgBL') as testcol2,"
                + " avg(event_id) as testcol3) as heartbeat"
                + " where heartbeat.$row_status is new;";
    }

    @Override
    protected void beforeQueryParse() {
        super.beforeQueryParse();

        try {
            cruncher.registerProvider("DiffBaseline/CountBL", CountDiffBaselineProvider.class);
            cruncher.registerProvider("DiffBaseline/AvgBL", StatsDiffBaselineProvider.class);
        }
        catch (StreamCruncherException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Event[] createEventArray() {
        return eventSequence;
    }

    @Override
    protected void verify(List<BatchResult> results) {
        String hash = "";

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
                 * "levela", "levelb", "change", "change2","beats", "testcol1",
                 * "testcol2", "testcol3".
                 */
                String levela = (String) objects[0];
                String levelb = (String) objects[1];
                Integer change = objects[2] == null ? null : ((Number) objects[2]).intValue();
                Integer change2 = objects[3] == null ? null : ((Number) objects[3]).intValue();
                Integer beats = objects[4] == null ? null : ((Number) objects[4]).intValue();
                Double testcol1 = objects[5] == null ? null : ((Number) objects[5]).doubleValue();
                Double testcol2 = objects[6] == null ? null : ((Number) objects[6]).doubleValue();
                Double testcol3 = objects[7] == null ? null : ((Number) objects[7]).doubleValue();

                hash = levela + " " + levelb + " " + change + " " + change2 + " " + beats + " "
                        + testcol1 + " " + testcol2 + " " + testcol3;

                System.out.println();
            }
        }

        String expectedHash = "datacntr1 ultrasparc14 -1 0 0 -5.0 0.0 null";
        Assert.assertEquals(hash, expectedHash, "Last entry does not match expectations");
    }

    /**
     * Custom Provider for the <code>count</code> function. Therefore, works
     * only on {@link Integer}s.
     */
    public static class CountDiffBaselineProvider extends DiffBaselineProvider<Integer> {
        public CountDiffBaselineProvider() {
            super();

            System.out.println("Using CountDiffBaselineProvider");
        }

        /**
         * @return Fixed Baseline. Always Zero.
         */
        @Override
        public Integer getBaseline(Integer oldValue, Integer newValue) {
            return 0;
        }
    }

    /**
     * Custom Provider for the Statistics functions (<code>avg,variance..</code>).
     * Therefore, works only on {@link Double}s.
     */
    public static class StatsDiffBaselineProvider extends DiffBaselineProvider<Double> {
        public StatsDiffBaselineProvider() {
            super();

            System.out.println("Using StatsDiffBaselineProvider");
        }

        /**
         * @return Fixed Baseline. Always Zero.
         */
        @Override
        public Double getBaseline(Double oldValue, Double newValue) {
            return 0.0;
        }
    }
}
