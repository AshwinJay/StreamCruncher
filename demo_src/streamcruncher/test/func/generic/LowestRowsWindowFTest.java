package streamcruncher.test.func.generic;

import java.sql.Timestamp;
import java.util.List;

import org.testng.Assert;

import streamcruncher.api.artifact.RowSpec;
import streamcruncher.api.artifact.RowSpec.Info;
import streamcruncher.test.func.BatchResult;

/*
 * Author: Ashwin Jayaprakash Date: Sep 10, 2006 Time: 4:49:51 PM
 */
/**
 * Test for the "Lowest X" in a Partition Window clause.
 */
public abstract class LowestRowsWindowFTest extends HighestRowsWindowFTest {
    @Override
    protected String getRQL() {
        return "select event_id, vehicle_id, speed from test (partition store lowest " + windowSize
                + " using event_id) as testStr "
                + "where testStr.$row_status is not dead order by event_id;";
    }

    @Override
    protected String[] getColumnTypes() {
        return new String[] { java.lang.Long.class.getName(), java.sql.Timestamp.class.getName(),
                RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 12),
                java.lang.Integer.class.getName(), java.lang.Double.class.getName() };
    }

    @Override
    protected String[] getResultColumnTypes() {
        return new String[] { java.lang.Long.class.getName(),
                RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 12),
                java.lang.Double.class.getName() };
    }

    @Override
    protected void verify(List<BatchResult> results) {
        long[] resultValues = { 1, 2, 3, 4, 5 };

        System.out.println("--Results--");
        for (BatchResult result : results) {
            System.out.println("Batch created at: " + new Timestamp(result.getTimestamp())
                    + ". Rows: " + result.getRows().size());

            List<Object[]> rows = result.getRows();

            int m = 0;
            System.out.println("  Batch results");
            for (Object[] objects : rows) {
                System.out.print("  ");
                for (Object object : objects) {
                    System.out.print(object + " ");
                }
                System.out.println();

                /*
                 * "event_id", "vehicle_id", "speed".
                 */

                Long id = ((Number) objects[0]).longValue();
                Assert.assertEquals(resultValues[m], id.longValue(), "Row value does not match");
                m++;
            }
        }
    }
}
