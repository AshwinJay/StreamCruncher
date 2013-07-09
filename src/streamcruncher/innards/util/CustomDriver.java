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
package streamcruncher.innards.util;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

import streamcruncher.boot.ConfigKeys;

/*
 * Author: Ashwin Jayaprakash Date: Jan 6, 2007 Time: 12:30:06 AM
 */

public class CustomDriver implements Driver {
    public static final String CUSTOM_DRIVER_URL = "jdbc:streamcruncher:custom";

    protected final Driver realDriver;

    protected final String realDriverURL;

    protected final Properties internalProperties;

    public CustomDriver(Properties properties) throws InstantiationException,
            IllegalAccessException, ClassNotFoundException {
        String driverClassName = properties.getProperty(ConfigKeys.DB.DRIVER_CLASS_NAME);
        this.realDriver = (Driver) Class.forName(driverClassName).newInstance();

        this.realDriverURL = properties.getProperty(ConfigKeys.DB.DRIVER_URL);

        String user = properties.getProperty(ConfigKeys.DB.USER);
        String password = properties.getProperty(ConfigKeys.DB.PASSWORD);
        this.internalProperties = new Properties(properties);
        this.internalProperties.setProperty("user", user);
        this.internalProperties.setProperty("password", password);

        // ------------

        /*
         * Overwrite the original settings so that further requests will go
         * through this Driver Adapter.
         */
        properties.setProperty(ConfigKeys.DB.DRIVER_CLASS_NAME, getClass().getName());
        properties.setProperty(ConfigKeys.DB.DRIVER_URL, CUSTOM_DRIVER_URL);
    }

    public boolean acceptsURL(String url) throws SQLException {
        return CUSTOM_DRIVER_URL.equals(url);
    }

    public Connection connect(String url, Properties info) throws SQLException {
        // Ignore the params because we have switched them.
        Connection connection = realDriver.connect(realDriverURL, internalProperties);

        return wrapNewConnection(connection);
    }

    public int getMajorVersion() {
        return realDriver.getMajorVersion();
    }

    public int getMinorVersion() {
        return realDriver.getMinorVersion();
    }

    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return realDriver.getPropertyInfo(realDriverURL, internalProperties);
    }

    public boolean jdbcCompliant() {
        return realDriver.jdbcCompliant();
    }

    // -----------

    protected Connection wrapNewConnection(Connection connection) {
        return connection;
    }
}