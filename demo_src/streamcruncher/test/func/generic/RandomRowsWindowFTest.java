package streamcruncher.test.func.generic;

import java.sql.Timestamp;
import java.util.LinkedHashSet;
import java.util.List;

import org.testng.Assert;

import streamcruncher.api.artifact.RowSpec;
import streamcruncher.api.artifact.RowSpec.Info;
import streamcruncher.test.func.BatchResult;
import streamcruncher.test.func.TrafficGenerator;

/*
 * Author: Ashwin Jayaprakash Date: Sep 10, 2006 Time: 4:49:51 PM
 */
/**
 * This Test demonstrates the "Random X" Window Feature.
 */
public abstract class RandomRowsWindowFTest extends TrafficGenerator {
    protected static final int windowSize = 5;

    protected int getMaxDataRows() {
        return 30;
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

    protected void afterEvent(int counter) {
        if (counter % 10 == 0) {
            try {
                Thread.sleep(5000);
            }
            catch (InterruptedException e) {
                e.printStackTrace(System.err);
            }
        }
    }

    @Override
    protected String getRQL() {
        return "select event_id, vehicle_id, speed from test (partition store random " + windowSize
                + ") as testStr where testStr.$row_status is new;";
    }

    @Override
    protected void verify(List<BatchResult> results) {
        LinkedHashSet<Long> idsInWindow = new LinkedHashSet<Long>();

        System.out.println("--Results--");
        for (BatchResult result : results) {
            System.out.println("Batch created at: " + new Timestamp(result.getTimestamp())
                    + ". Rows: " + result.getRows().size());

            List<Object[]> rows = result.getRows();

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
                Assert.assertFalse(idsInWindow.contains(id), "Id already exists in the Window");

                idsInWindow.add(id);
            }
        }
    }
}
