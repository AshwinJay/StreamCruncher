package streamcruncher.test.func.generic;

import streamcruncher.api.artifact.RowSpec;
import streamcruncher.api.artifact.RowSpec.Info;
import streamcruncher.test.func.TumblingWindowTest;

/*
 * Author: Ashwin Jayaprakash Date: Sep 10, 2006 Time: 4:49:51 PM
 */

/**
 * A simple Test to demonstrate the "Latest Rows Window" in an anonymous
 * Partition.
 */
public abstract class TumblingWindowFTest extends TumblingWindowTest {
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
    protected String getRQL() {
        return "select event_id, vehicle_id, speed "
                + "from test (partition store latest 5) as testStr "
                + "where testStr.$row_status is not dead;";
    }
}
