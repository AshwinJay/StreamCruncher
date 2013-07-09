package streamcruncher.test.func.generic;

import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Iterator;

import streamcruncher.api.artifact.RowSpec;
import streamcruncher.api.artifact.RowSpec.Info;

/*
 * Author: Ashwin Jayaprakash Date: Mar 14, 2007 Time: 8:10:02 PM
 */

/**
 * This Test demonstrates some of the more advanced features of the Alert-Query
 * by making use of Chained Partitions and an additional <code>where</code>
 * clause as a Post-Filter on the <code>alert</code> clause.
 */
public abstract class MultiStreamEventGeneratorChainTest extends MultiStreamEventGeneratorTest {
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

                new Event(EventType.pause, new Object[] { 4000L }, -1),
                new Event(EventType.stg3_event, new Object[] { "TX", "Dallas", null, 1100L }, 3),
                new Event(EventType.stg3_event, new Object[] { "TX", "Dallas", null, 1200L }, 3),
                new Event(EventType.stg3_event, new Object[] { "TX", "Dallas", null, 1300L }, 3),
                new Event(EventType.stg3_event, new Object[] { "TX", "Dallas", null, 1500L }, 3),
                new Event(EventType.pause, new Object[] { 2500L }, -1),
                new Event(EventType.stg2_event, new Object[] { "TX", "Dallas", null, 1100L }, 3),
                new Event(EventType.stg2_event, new Object[] { "TX", "Dallas", null, 1200L }, 3),
                new Event(EventType.stg2_event, new Object[] { "TX", "Dallas", null, 1400L }, 3),
                new Event(EventType.stg2_event, new Object[] { "TX", "Dallas", null, 1500L }, 3),
                new Event(EventType.pause, new Object[] { 250L }, -1),
                new Event(EventType.stg1_event, new Object[] { "TX", "Dallas", null, 1100L }, 3),
                new Event(EventType.stg1_event, new Object[] { "TX", "Dallas", null, 1200L }, 3),
                new Event(EventType.stg1_event, new Object[] { "TX", "Dallas", null, 1300L }, 3),
                new Event(EventType.stg1_event, new Object[] { "TX", "Dallas", null, 1400L }, 3),
                new Event(EventType.stg1_event, new Object[] { "TX", "Dallas", null, 1500L }, 3) };
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
    protected Iterator<Event> getEvents() {
        final Event[] eventSequenceNew = createEventArray();

        Iterator<Event> iter = new Iterator<Event>() {
            int counter = 0;

            public boolean hasNext() {
                return !(counter == eventSequenceNew.length);
            }

            public Event next() {
                Event retVal = eventSequenceNew[counter++];

                Object[] data = retVal.getData();
                for (int i = 0; i < data.length; i++) {
                    if (data[i] == null) {
                        data[i] = new Timestamp(System.currentTimeMillis());
                    }
                }

                for (Object object : data) {
                    System.out.print(object + " ");
                }
                System.out.println();

                return retVal;
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }
        };

        return iter;
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
                + " using stg1_event (partition by levela, levelb store last 5 seconds) as one_event correlate on event_id,"
                // 2 Partition levels just to test Chained Partitions.
                + " stg2_event (partition by levela, levelb store last 5 seconds) to"
                + " (partition store last 8 seconds where $row_status is new and levelb = 'Austin') as two_event correlate on event_id,"
                + " stg3_event (partition by levela, levelb store last 5 seconds) to"
                + " (partition store last 8 seconds where $row_status is new and levelb = 'Austin') as three_event correlate on event_id"

                // For "Austin" events.
                + " when present(one_event and two_event and not three_event)"
                // For "Austin" events.
                + " or present(one_event and not two_event and three_event)"
                // For "Dallas" events.
                + " or present(one_event and not two_event and not three_event)"
                // For "Austin" events.
                + " or present(one_event and two_event and three_event)" +

                // Post "alert" filtering.
                " where stg2_id is null;";
    }

    @Override
    protected HashSet<String> getExpectedResults() {
        HashSet<String> expectedResults = new HashSet<String>();

        expectedResults.add("700 null 700 Austin");
        expectedResults.add("800 null 800 Austin");
        expectedResults.add("1000 null 1000 Austin");

        expectedResults.add("1100 null null Dallas");
        expectedResults.add("1200 null null Dallas");
        expectedResults.add("1300 null null Dallas");
        expectedResults.add("1400 null null Dallas");
        expectedResults.add("1500 null null Dallas");

        return expectedResults;
    }
}
