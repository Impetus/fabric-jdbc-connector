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

    private static final int MAJOR_VERSION = 1;

    private static final int MINOR_VERSION = 1;

    private static Pattern pattern = Pattern.compile("jdbc:fabric://(.*)");

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
        if (url == null || !url.startsWith("jdbc:fabric")) {
            return false;
        }
        return true;
    }

    public Connection connect(String url, Properties info) throws SQLException {
        Properties props = parseURL(url, info);
        return new FabricConnection(url, props);
    }

    public int getMajorVersion() {
        return MAJOR_VERSION;
    }

    public int getMinorVersion() {
        return MINOR_VERSION;
    }

    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        // TODO Auto-generated method stub
        return null;
    }

    public DriverPropertyInfo[] getPropertyInfo(String arg0, Properties arg1) throws SQLException {
        // TODO Auto-generated method stub
        return null;
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
        if (m.matches() && m.groupCount() == 1) {
            props.put("configPath", m.group(1).trim());
        } else {
            throw new RuntimeException("fabric jdbc url is wrong");
        }
        return props;
    }

}
