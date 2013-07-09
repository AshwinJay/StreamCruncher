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
 * Author: Ashwin Jayaprakash Date: Jul 17, 2007 Time: 6:24:04 PM
 */

/**
 * <p>
 * This example demonstrates a scenario where there are 3 Event Streams. An
 * Event from Stream 1 must be followed by an Event from Stream 2 and 3 within 5
 * seconds of occurence of the Stream 1 Event. Stream 2 & 3 Events can arrive in
 * any order. But they must arrive after Stream 1's Event.
 * </p>
 */
public abstract class ThreeEventOrderTest extends MultiStreamEventGenerator {
    @Override
    protected Event[] createEventArray() {
        return new Event[] {
                // Timestamp column will be filled later.

                // All 3 together.
                new Event(EventType.stg1_event, new Object[] { "One", "Hello", null, 100L }, 3),
                new Event(EventType.stg2_event, new Object[] { "Two", "Hi", null, 100L }, 3),
                new Event(EventType.stg3_event, new Object[] { "Three", "Howdy", null, 100L }, 3),

                new Event(EventType.pause, new Object[] { 2500L }, -1),

                // 1 & 2 together. 3 is very late.
                new Event(EventType.stg1_event, new Object[] { "One", "Hello", null, 200L }, 3),
                new Event(EventType.stg2_event, new Object[] { "Two", "Hi", null, 200L }, 3),
                new Event(EventType.pause, new Object[] { 5200L }, -1),
                new Event(EventType.stg3_event, new Object[] { "Three", "Howdy", null, 200L }, 3),

                new Event(EventType.pause, new Object[] { 2500L }, -1),

                // 2 & 3 together. 1 is very late.
                new Event(EventType.stg2_event, new Object[] { "Two", "Hi", null, 300L }, 3),
                new Event(EventType.stg3_event, new Object[] { "Three", "Howdy", null, 300L }, 3),
                new Event(EventType.pause, new Object[] { 5200L }, -1),
                new Event(EventType.stg1_event, new Object[] { "One", "Hello", null, 300L }, 3),

                new Event(EventType.pause, new Object[] { 2500L }, -1),

                // 1 comes first. 2 & 3 together, later.
                new Event(EventType.stg1_event, new Object[] { "One", "Hello", null, 400L }, 3),
                new Event(EventType.pause, new Object[] { 3000L }, -1),
                new Event(EventType.stg2_event, new Object[] { "Two", "Hi", null, 400L }, 3),
                new Event(EventType.stg3_event, new Object[] { "Three", "Howdy", null, 400L }, 3),

                new Event(EventType.pause, new Object[] { 2500L }, -1),

                // 1 comes first, 2 comes later. 3 never comes
                new Event(EventType.stg1_event, new Object[] { "One", "Hello", null, 500L }, 3),
                new Event(EventType.pause, new Object[] { 2500L }, -1),
                new Event(EventType.stg2_event, new Object[] { "Two", "Hi", null, 500L }, 3),

                new Event(EventType.pause, new Object[] { 2500L }, -1),

                // 2 comes first, 3 comes later. 1 comes after 3.
                new Event(EventType.stg2_event, new Object[] { "Two", "Hi", null, 600L }, 3),
                new Event(EventType.pause, new Object[] { 1500L }, -1),
                new Event(EventType.stg3_event, new Object[] { "Three", "Howdy", null, 600L }, 3),
                new Event(EventType.pause, new Object[] { 1500L }, -1),
                new Event(EventType.stg1_event, new Object[] { "One", "Hello", null, 600L }, 3),

                new Event(EventType.pause, new Object[] { 2500L }, -1),

                // 1 comes first, 3 comes later. 2 comes after 3.
                new Event(EventType.stg1_event, new Object[] { "One", "Hello", null, 700L }, 3),
                new Event(EventType.pause, new Object[] { 1500L }, -1),
                new Event(EventType.stg3_event, new Object[] { "Three", "Howdy", null, 700L }, 3),
                new Event(EventType.pause, new Object[] { 1500L }, -1),
                new Event(EventType.stg2_event, new Object[] { "Two", "Hi", null, 700L }, 3),

                new Event(EventType.pause, new Object[] { 2500L }, -1),

                // 1 comes first, 2 comes later. 3 comes soon after 2.
                new Event(EventType.stg1_event, new Object[] { "One", "Hello", null, 800L }, 3),
                new Event(EventType.pause, new Object[] { 1000L }, -1),
                new Event(EventType.stg2_event, new Object[] { "Two", "Hi", null, 800L }, 3),
                new Event(EventType.pause, new Object[] { 250L }, -1),
                new Event(EventType.stg3_event, new Object[] { "Three", "Howdy", null, 800L }, 3)
        // End test data.
        };
    }

    @Override
    protected String[] getResultColumnTypes() {
        return new String[] { java.lang.Long.class.getName(), java.sql.Timestamp.class.getName(),
                java.lang.Long.class.getName(), java.sql.Timestamp.class.getName(),
                java.lang.Long.class.getName(), java.sql.Timestamp.class.getName() };
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
        return new String[] { "stg1_id", "stg1_time", "stg2_id", "stg2_time", "stg3_id",
                "stg3_time" };
    }

    @Override
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

    @Override
    protected String getRQL() {
        String csv = getRQLColumnsCSV();

        String query = "select "
                + csv
                + "\n\n from"

                // The columns to spit out.
                + "\n alert one_event.event_id as stg1_id, one_event.event_time as stg1_time,"
                + " two_event.event_id as stg2_id, two_event.event_time as stg2_time,"
                + " three_event.event_id as stg3_id, three_event.event_time as stg3_time"

                // The source of the Events.
                + "\n\n using stg1_event (partition store last 5 seconds) as one_event correlate on event_id,"
                + " stg2_event (partition store last 5 seconds) as two_event correlate on event_id,"
                + " stg3_event (partition store last 5 seconds) as three_event correlate on event_id"

                /*
                 * The Conditions that the Query is supposed to watch for. All 3
                 * Events must arrive within the Window.
                 */
                + "\n\n when present(one_event and two_event and three_event)" +

                // Post filtering: Stage 1 comes before Stage 2 & 3.
                "\n\n where stg1_time < stg2_time and stg1_time < stg3_time;";

        System.out.println("========== Query ==========");
        System.out.println(query);
        System.out.println("===========================");

        return query;
    }

    @Override
    protected void verify(List<BatchResult> results) {
        HashSet<String> expectedResults = new HashSet<String>();
        expectedResults.add("400 400 400");
        expectedResults.add("700 700 700");
        expectedResults.add("800 800 800");

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
                 * "stg1_id", "stg1_time", "stg2_id", "stg2_time", "stg3_id",
                 * "stg3_time".
                 */
                Long stg1Id = objects[0] == null ? null : ((Number) objects[0]).longValue();
                Timestamp stg1TS = objects[1] == null ? null : ((Timestamp) objects[1]);
                Long stg2Id = objects[2] == null ? null : ((Number) objects[2]).longValue();
                Timestamp stg2TS = objects[3] == null ? null : ((Timestamp) objects[3]);
                Long stg3Id = objects[4] == null ? null : ((Number) objects[4]).longValue();
                Timestamp stg3TS = objects[5] == null ? null : ((Timestamp) objects[5]);

                String hash = stg1Id + " " + stg2Id + " " + stg3Id;
                boolean exists = expectedResults.remove(hash);

                if (exists == false) {
                    /*
                     * Sometimes the Stage 1 of the first Event set(Id: 100)
                     * reaches the Kernel very quickly, even though all 3 are
                     * sent almost simultaneously. So, this is excusable.
                     */
                    Assert.assertEquals(hash, "100 100 100", "Only Event Id set 100 can arrive");
                    exists = true;
                }

                Assert.assertTrue(exists, "Pattern " + hash + " does not match expected results");

                Assert.assertTrue((stg1TS.getTime() < stg2TS.getTime()),
                        "Stage 2 arrived before/with Stage 1");
                Assert.assertTrue((stg1TS.getTime() < stg3TS.getTime()),
                        "Stage 3 arrived before/with Stage 1");

                System.out.println();
            }
        }

        Assert.assertEquals(expectedResults.size(), 0, "All expected Patterns did not arrive");
    }
}