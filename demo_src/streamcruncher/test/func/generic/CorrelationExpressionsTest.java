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
 * Test complex expression evaluation in Correlations.
 */
public abstract class CorrelationExpressionsTest extends MultiStreamEventGenerator {
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
                new Event(EventType.stg3_event, new Object[] { "TX", "Dallas", null, 500L }, 3),
                new Event(EventType.stg3_event, new Object[] { "TX", "Austin", null, 600L }, 3),
                new Event(EventType.stg3_event, new Object[] { "TX", "Austin", null, 700L }, 3),
                new Event(EventType.stg3_event, new Object[] { "TX", "Austin", null, 800L }, 3),
                new Event(EventType.stg3_event, new Object[] { "TX", "Austin", null, 900L }, 3),
                new Event(EventType.stg3_event, new Object[] { "TX", "Austin", null, 1000L }, 3),
                new Event(EventType.pause, new Object[] { 2500L }, -1),
                new Event(EventType.stg2_event, new Object[] { "TX", "Dallas", null, 500L }, 3),
                new Event(EventType.stg2_event, new Object[] { "TX", "Austin", null, 600L }, 3),
                new Event(EventType.stg2_event, new Object[] { "TX", "Austin", null, 900L }, 3),
                new Event(EventType.pause, new Object[] { 250L }, -1),
                new Event(EventType.stg1_event, new Object[] { "TX", "Dallas", null, 500L }, 3),
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
                java.lang.Long.class.getName(), java.lang.Double.class.getName(),
                RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 20),
                java.sql.Timestamp.class.getName() };
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
        return new String[] { "stg1_id", "stg2_id", "stg3_id", "expr", "comments",
                "current_timestamp" };
    }

    @Override
    protected String getRQLColumnsCSV() {
        String[] columns = getResultColumnNames();

        // Insert a complex expression column as "expr".
        /*
         * todo OGNL problem: (((stg1_id * stg2_id)/10) * stg3_id) + (3000 -
         * 200)
         */
        columns[3] = "stg1_id + stg2_id + stg3_id + (3000 - 200) as " + columns[3];

        // Insert a case..when script pseudo column at the end as "comments".
        columns[4] = "case"
                + " when stg2_id = 200 then 'Stage 2 is 200!'"
                + " when stg3_id > 1100 then 'Stage 3 is above 1100!'"
                + " when stg1_id is not null and stg2_id = 300 then 'Stage 1 is not null and Stage 2 is 300!'"
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

        return "select first 30 "
                + csv
                + " from"

                + " alert one_event.event_id as stg1_id,"
                + " two_event.event_id as stg2_id,"
                + " three_event.event_id as stg3_id,"
                + " one_event.levelb as levelb"
                + " using stg1_event (partition store last 5 seconds) as one_event correlate on event_id,"
                + " stg2_event (partition store last 5 seconds) as two_event correlate on event_id,"
                + " stg3_event (partition store last 5 seconds) as three_event correlate on event_id"

                + " when present(one_event and two_event and three_event)"

                /*
                 * todo OGNL problem: ((stg1_id * 2) + 8) = (stg2_id *
                 * 1000/1000) + (stg3_id + 8))
                 */
                + " where (stg1_id + 8 = stg2_id + 8) and (levelb = 'Auckland' or levelb = 'Austin');";
    }

    protected HashSet<String> getExpectedResults() {
        HashSet<String> expectedResults = new HashSet<String>();

        expectedResults.add("100 100 100");
        expectedResults.add("400 400 400");
        expectedResults.add("600 600 600");
        expectedResults.add("1200 1200 1200");

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
                 * "stg1_id", "stg2_id", "stg3_id", "expr", "comment",
                 * "timestamp".
                 */
                Long stg1Id = objects[0] == null ? null : ((Number) objects[0]).longValue();
                Long stg2Id = objects[1] == null ? null : ((Number) objects[1]).longValue();
                Long stg3Id = objects[2] == null ? null : ((Number) objects[2]).longValue();

                Assert.assertNotNull(objects[3], "Expression should not've been null");
                Assert.assertNotNull(objects[4], "Comment should not've been null");
                Assert.assertNotNull(objects[5], "Timestamp should not've been null");

                String hash = stg1Id + " " + stg2Id + " " + stg3Id;
                boolean exists = expectedResults.remove(hash);
                Assert.assertTrue(exists, "Pattern " + hash + " does not match expected results");

                System.out.println();
            }
        }

        Assert.assertEquals(expectedResults.size(), 0, "All expected Patterns did not arrive");
    }
}
