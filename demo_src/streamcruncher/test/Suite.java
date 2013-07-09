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

import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import streamcruncher.api.StreamCruncher;

/*
 * Author: Ashwin Jayaprakash Date: Sep 10, 2006 Time: 12:36:35 PM
 */

public class Suite {
    @BeforeSuite(groups = { TestGroupNames.SC_INIT_REQUIRED }, alwaysRun = true)
    public void start() throws Exception {
        String prop = System.getProperty("sc.config.file");

        StreamCruncher cruncher = new StreamCruncher();
        cruncher.start(prop);

        // Test startup.
        Connection connection = cruncher.createConnection();
        connection.close();
    }

    @Test(groups = { TestGroupNames.SC_INIT_REQUIRED }, alwaysRun = true)
    public void dummyTest() {
    }

    @AfterSuite(groups = { TestGroupNames.SC_INIT_REQUIRED }, alwaysRun = true)
    public void end() throws Exception {
        new StreamCruncher().stop();
    }
}
