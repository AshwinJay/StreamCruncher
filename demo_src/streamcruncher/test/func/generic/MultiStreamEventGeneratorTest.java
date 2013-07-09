package streamcruncher.test.func.generic;

import java.sql.Timestamp;
import java.util.HashSet;
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
 * This Test demonstrates the use of the Pattern matching features (Multi-Stream
 * Correlation). Events arrive from 3 different Streams, which simulates Objects
 * bearing a unique numbers and passing through 3 different stages. The
 * <code>when..present(..)..</code> clause is used to succinctly describe the
 * Patterns to watch for.
 * </p>
 * <p>
 * 3 "watch-patterns" are defined on the 3 Streams. A 5 second Partition Window
 * is defined on each of the 3 Streams.
 * </p>
 * <p>
 * Pattern 1) An Event appears in Stage1 and then it arrives in Stage2 but never
 * arrives in Stage3. Even if it does, the Event has expired from Stages 1
 * and/or 2.
 * </p>
 * <p>
 * Pattern 2) An Event appears in Stage1 and then it arrives in Stage3 but never
 * arrives in Stage2. Even if it does, the Event has expired from Stages 1
 * and/or 3.
 * </p>
 * <p>
 * Pattern 3) An Event appears in Stage1, Stage2 and Stage3 - all before it
 * expires from any of the 3 Stages.
 * </p>
 * To demonstrate that the other SQL features can still be used in the
 * Alert-Query, the Select clause uses a <code>case..when</code> clause to add
 * a comment based on the absence/presence of columns.
 */
public abstract class MultiStreamEventGeneratorTest extends MultiStreamEventGenerator {
    @Override
    protected Event[] createEventArray() {
        /**
         * Timestamp column will be filled later.
         */
        return new Event[] {
                new Event(EventType.stg1_event, new Object[] { "TX", "Austin", null, 100L }, 3),
                new Event(EventType.stg1_event, new Object[] { "TX", "Austin", null, 200L }, 3),
                new Event(EventType.stg1_event, new Object[] { "TX", "Austin", null, 300L }, 3),
                new Event(EventType.stg1_event, new Object[] { "TX", "Austin", null, 400L }, 3),
                new Event(EventType.pause, new Object[] { 2500L }, -1),
                new Event(EventType.stg2_event, new Object[] { "TX", "Austin", null, 100L }, 3),
                new Event(EventType.stg2_event, new Object[] { "TX", "Austin", null, 200L }, 3),
                new Event(EventType.stg2_event, new Object[] { "TX", "Austin", null, 300L }, 3),
                new Event(EventType.stg2_event, new Object[] { "TX", "Austin", null, 400L }, 3),
                new Event(EventType.pause, new Object[] { 250L }, -1),
                new Event(EventType.stg3_event, new Object[] { "TX", "Austin", null, 100L }, 3),
                new Event(EventType.stg3_event, new Object[] { "TX", "Austin", null, 400L }, 3),
                new Event(EventType.pause, new Object[] { 4000L }, -1),
                new Event(EventType.stg3_event, new Object[] { "TX", "Austin", null, 500L }, 3),
                new Event(EventType.stg3_event, new Object[] { "TX", "Austin", null, 600L }, 3),
                new Event(EventType.stg3_event, new Object[] { "TX", "Austin", null, 700L }, 3),
                new Event(EventType.stg3_event, new Object[] { "TX", "Austin", null, 800L }, 3),
                new Event(EventType.stg3_event, new Object[] { "TX", "Austin", null, 900L }, 3),
                new Event(EventType.stg3_event, new Object[] { "TX", "Austin", null, 1000L }, 3),
                new Event(EventType.pause, new Object[] { 2500L }, -1),
                new Event(EventType.stg2_event, new Object[] { "TX", "Austin", null, 500L }, 3),
                new Event(EventType.stg2_event, new Object[] { "TX", "Austin", null, 600L }, 3),
                new Event(EventType.stg2_event, new Object[] { "TX", "Austin", null, 900L }, 3),
                new Event(EventType.pause, new Object[] { 250L }, -1),
                new Event(EventType.stg1_event, new Object[] { "TX", "Austin", null, 500L }, 3),
                new Event(EventType.stg1_event, new Object[] { "TX", "Austin", null, 600L }, 3),
                new Event(EventType.stg1_event, new Object[] { "TX", "Austin", null, 700L }, 3),
                new Event(EventType.stg1_event, new Object[] { "TX", "Austin", null, 800L }, 3),
                new Event(EventType.stg1_event, new Object[] { "TX", "Austin", null, 1000L }, 3),

                new Event(EventType.pause, new Object[] { 2500L }, -1),
                new Event(EventType.stg3_event, new Object[] { "TX", "Austin", null, 1100L }, 3),
                new Event(EventType.stg1_event, new Object[] { "TX", "Austin", null, 1100L }, 3),
                new Event(EventType.pause, new Object[] { 6500L }, -1),
                // Stage1 arrives, but not in time.
                new Event(EventType.stg2_event, new Object[] { "TX", "Austin", null, 1100L }, 3),

                new Event(EventType.pause, new Object[] { 2500L }, -1),
                new Event(EventType.stg3_event, new Object[] { "TX", "Austin", null, 1200L }, 3),
                new Event(EventType.stg2_event, new Object[] { "TX", "Austin", null, 1200L }, 3),
                new Event(EventType.stg1_event, new Object[] { "TX", "Austin", null, 1200L }, 3) };
    }

    @Override
    protected String[] getResultColumnTypes() {
        return new String[] { java.lang.Long.class.getName(), java.lang.Long.class.getName(),
                java.lang.Long.class.getName(),
                RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 10),
                RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 10) };
    }

    @Override
    protected String[] getStage1ColumnTypes() {
        return new String[] { RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 10),
                RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 10),
                java.sql.Timestamp.class.getName(), java.lang.Long.class.getName() };
    }

    @Override
    protected String[] getStage2ColumnTypes() {
        return new String[] { RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 10),
                RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 10),
                java.sql.Timestamp.class.getName(), java.lang.Long.class.getName() };
    }

    @Override
    protected String[] getStage3ColumnTypes() {
        return new String[] { RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 10),
                RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 10),
                java.sql.Timestamp.class.getName(), java.lang.Long.class.getName() };
    }

    @Override
    protected String[] getResultColumnNames() {
        return new String[] { "stg1_id", "stg2_id", "stg3_id", "levelb", "comments" };
    }

    @Override
    protected String getRQLColumnsCSV() {
        String[] columns = getResultColumnNames();

        // Insert a case..when script pseudo column at the end as "comments".
        columns[4] = "case"
                + " when stg2_id is null and stg3_id is not null then 'Stage 2 missing!'"
                + " when stg2_id is not null and stg3_id is null then 'Stage 3 missing!'"
                + " when stg2_id is null and stg3_id is null then 'Stage 2 & 3 missing!'"
                + " else 'All OK!' end as " + columns[4];

        String csv = "";
        for (int i = 0; i < columns.length; i++) {
            if (csv != "") {
                csv = csv + ", ";
            }
            csv = csv + columns[i];
        }

        return csv;
    }

    @Override
    protected String getRQL() {
        String csv = getRQLColumnsCSV();

        return "select "
                + csv
                + " from"
                + " alert one_event.event_id as stg1_id,"
                + " two_event.event_id as stg2_id,"
                + " three_event.event_id as stg3_id,"
                + " one_event.levelb as levelb"
                + " using stg1_event (partition store last 5 seconds) as one_event correlate on event_id,"
                + " stg2_event (partition store last 5 seconds) as two_event correlate on event_id,"
                + " stg3_event (partition store last 5 seconds) as three_event correlate on event_id"

                + " when present(one_event and two_event and not three_event)"
                + " or present(one_event and not two_event and three_event)"
                + " or present(one_event and two_event and three_event);";
    }

    protected HashSet<String> getExpectedResults() {
        HashSet<String> expectedResults = new HashSet<String>();

        expectedResults.add("100 100 100 Austin");
        expectedResults.add("200 200 null Austin");
        expectedResults.add("300 300 null Austin");
        expectedResults.add("400 400 400 Austin");
        expectedResults.add("500 500 500 Austin");
        expectedResults.add("600 600 600 Austin");
        expectedResults.add("700 null 700 Austin");
        expectedResults.add("800 null 800 Austin");
        expectedResults.add("1000 null 1000 Austin");
        expectedResults.add("1100 null 1100 Austin");
        expectedResults.add("1200 1200 1200 Austin");

        return expectedResults;
    }

    @Override
    protected void verify(List<BatchResult> results) {
        HashSet<String> expectedResults = getExpectedResults();

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
                 * "stg1_id", "stg2_id", "stg3_id", "levelb", "comment".
                 */
                Long stg1Id = objects[0] == null ? null : ((Number) objects[0]).longValue();
                Long stg2Id = objects[1] == null ? null : ((Number) objects[1]).longValue();
                Long stg3Id = objects[2] == null ? null : ((Number) objects[2]).longValue();
                String levelb = (String) objects[3];

                String hash = stg1Id + " " + stg2Id + " " + stg3Id + " " + levelb;
                boolean exists = expectedResults.remove(hash);
                Assert.assertTrue(exists, "Pattern " + hash + " does not match expected results");

                System.out.println();
            }
        }

        Assert.assertEquals(expectedResults.size(), 0, "All expected Patterns did not arrive");
    }
}
