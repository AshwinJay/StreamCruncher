package streamcruncher.test.nonfunc.oraclett;

import streamcruncher.innards.core.partition.PartitionDescender;
import streamcruncher.innards.core.partition.function.Function;
import streamcruncher.test.func.oraclett.OracleTimeWF1ChainedPartitionTest;

/*
 * Author: Ashwin Jayaprakash Date: Oct 29, 2006 Time: 11:54:29 AM
 */

public class OracleFunctionGCTest extends OracleTimeWF1ChainedPartitionTest {
    /**
     * Note: Add this line to {@link PartitionDescender#attemptCleanup()}
     * 
     * <pre>
     * System.err.println(&quot;Map after cleanup: &quot; + System.identityHashCode(map) + &quot;, Contents: &quot; + map);
     * </pre>
     * 
     * Note: Add this method to {@link Function}.
     * 
     * <pre>
     * @Override
     * protected void finalize() throws Throwable {
     *     super.finalize();
     * 
     *     System.err.println(&quot;Finalizing: &quot; + this + &quot;, home: &quot; + getHomeFunction());
     * }
     * </pre>
     */
    protected void afterEvent(int counter) {
        if (counter == 20) {
            try {
                System.err.println("Sleeping..");
                Thread.sleep(10000);
            }
            catch (InterruptedException e) {
                e.printStackTrace(System.err);
            }

            // ---------

            // Force heap growth.
            for (int i = 0; i < 100000; i++) {
                new Integer(i);
            }

            System.err.println("Running GC");
            System.gc();
        }
    }
}
