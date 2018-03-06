/*******************************************************************************
* * Copyright 2017 Impetus Infotech.
* *
* * Licensed under the Apache License, Version 2.0 (the "License");
* * you may not use this file except in compliance with the License.
* * You may obtain a copy of the License at
* *
* * http://www.apache.org/licenses/LICENSE-2.0
* *
* * Unless required by applicable law or agreed to in writing, software
* * distributed under the License is distributed on an "AS IS" BASIS,
* * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* * See the License for the specific language governing permissions and
* * limitations under the License.
******************************************************************************/
package com.impetus.fabric.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Enumeration;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.LoggerFactory;

import com.impetus.blkch.jdbc.BlkchnDriver;

public class FabricDriver implements BlkchnDriver {

    private static Pattern pattern = Pattern.compile(DriverConstants.DRIVER_PREFIX + "://([^:]*):(.*)");

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(FabricDriver.class);

    static {
        try {
            DriverManager.registerDriver(new FabricDriver());
        } catch (SQLException e) {
            LOGGER.error("Failed to register Fabric Driver", e);
        }
    }

    private FabricDriver() {
        // Prevents Instantiation
    }

    public boolean acceptsURL(String url) throws SQLException {
        if (url == null || !url.startsWith(DriverConstants.DRIVER_PREFIX)) {
            return false;
        }
        return true;
    }

    public Connection connect(String url, Properties info) throws SQLException {
        Properties props = parseURL(url, info);
        return new FabricConnection(url, props);
    }

    public int getMajorVersion() {
        return DriverConstants.MAJOR_VERSION;
    }

    public int getMinorVersion() {
        return DriverConstants.MINOR_VERSION;
    }

    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new UnsupportedOperationException();
    }

    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean jdbcCompliant() {
        return false;
    }

    private Properties parseURL(String url, Properties info) {
        Properties props = new Properties();
        for (Enumeration<?> e = info.propertyNames(); e.hasMoreElements();) {
            String key = (String) e.nextElement();
            String value = info.getProperty(key);
            if (value != null) {
                props.put(key.toUpperCase(), value);
            }
        }
        Matcher m = pattern.matcher(url);
        if (m.matches() && m.groupCount() == 2) {
            props.put("configPath", m.group(1).trim());
            props.put("channel", m.group(2).trim());
        } else {
            throw new RuntimeException("fabric jdbc url is wrong");
        }
        return props;
    }

}
