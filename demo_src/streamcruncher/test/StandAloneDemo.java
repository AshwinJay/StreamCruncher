/*
 * StreamCruncher:  Copyright (c) 2006-2008, Ashwin Jayaprakash. All Rights Reserved.
 * Contact:         ashwin {dot} jayaprakash {at} gmail {dot} com
 * Web:             http://www.StreamCruncher.com
 *
 * This file is part of StreamCruncher.
 *
 *     StreamCruncher is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     StreamCruncher is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with StreamCruncher. If not, see <http://www.gnu.org/licenses/>.
 */
package streamcruncher.test;

import java.sql.Connection;

import streamcruncher.api.StreamCruncher;
import streamcruncher.test.func.h2.H2StockPriceComparisonTest;

/*
 * Author: Ashwin Jayaprakash Date: Apr 7, 2007 Time: 3:41:03 PM
 */

/**
 * <p>
 * Demo class that runs a single Test using the Java main(...) method instead of
 * using the TestNG Harness.
 * </p>
 * <b>Note:</b> Make sure that the appripriate Properties file is set in the
 * System Properties:
 * <code>-Dsc.config.file=./resources/sc_config_h2.properties</code>
 */
public class StandAloneDemo {
    public static void main(String[] args) {
        StandAloneDemo demo = new StandAloneDemo();

        try {
            demo.start();

            demo.runATest();
        }
        catch (Exception e) {
            System.err.println("Start up error");
            e.printStackTrace(System.err);
        }
        finally {
            try {
                demo.end();
            }
            catch (Exception e) {
                System.err.println("Shutdown error");
                e.printStackTrace(System.err);
            }
        }
    }

    public void start() throws Exception {
        String prop = System.getProperty("sc.config.file");

        StreamCruncher cruncher = new StreamCruncher();
        cruncher.start(prop);

        // Test startup.
        Connection connection = cruncher.createConnection();
        connection.close();
    }

    public void runATest() throws Exception {
        /*
         * Create an anonymous inner, sub-class because the methods are
         * protected. Inherit the DB specific TestCase for which the Test must
         * be run and invoke the test method.
         */
        new H2StockPriceComparisonTest() {
            public void performTest() throws Exception {
                super.init();
                super.test();
                super.discard();
            }
        }.performTest();
    }

    public void end() throws Exception {
        new StreamCruncher().stop();
    }
}
