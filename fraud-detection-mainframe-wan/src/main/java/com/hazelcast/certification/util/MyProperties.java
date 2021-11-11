package com.hazelcast.certification.util;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class MyProperties {

    private static final ILogger log = Logger.getLogger(MyProperties.class);

    public static String SERVER_IP;
    public static String SERVER_PORT;

    public static boolean WAN_ENABLED;
    public static String WAN_TARGET_URL;
    public static String WAN_TARGET_NAME;

    public static int TRANSACTION_COUNT;
    public static int MERCHANT_COUNT;
    public static int ACCOUNT_COUNT;

    public static String JDBC_DRIVER_CLASS;
    public static String JDBC_DB_NAME;
    public static String JDBC_PROTOCOL;
    public static String JDBC_HOST;
    public static String JDBC_PORT;
    public static String JDBC_USER;
    public static String JDBC_PASSWORD;
    public static String JDBC_TIMEZONE;
    public static boolean JDBC_AUTOCOMMIT;

    public static final String TXN_QUEUE_ID = "transaction_queue";
    public static final String ACCOUNT_MAP = "account";
    public static final String MERCHANT_MAP = "merchant";
    public static final String RULESRESULT_MAP = "rulesresult";

    static {
        loadProperties();
    }

    private static void loadProperties() {
        String propFileName = "Process.properties";
        InputStream stream = MyProperties.class.getClassLoader().getResourceAsStream(
                propFileName);
        if (null == stream) {
            try {
                throw new FileNotFoundException("Property file " + propFileName
                        + " not found in the classpath.  Defaults will be used");
            } catch (FileNotFoundException e) {
                log.severe(e);
            }
        }
        try {
            Properties properties = new Properties();
            properties.load(stream);
            setProperties(properties);
        } catch (IOException e) {
            log.severe(e);
        }
    }

    private static void setProperties(Properties properties) {
        SERVER_IP = properties.getProperty("ServerIP");
        if(SERVER_IP == null) {
            log.warning("Hazelcast server IP not configured, using localhost as default");
            SERVER_IP = "localhost";
        }

        SERVER_PORT = properties.getProperty("ServerPort");
        if(SERVER_PORT == null) {
            log.warning("Hazelcast server Port not configured, using 5701 as default");
            SERVER_PORT = "5701";
        }

        String wan_enable_string = properties.getProperty("WanEnable");
        if (wan_enable_string == null) {
            log.warning("WAN Replication configuration not found. WAN replication disabled.");
            WAN_ENABLED = false;
        } else
            WAN_ENABLED = Boolean.parseBoolean(wan_enable_string);

        WAN_TARGET_URL= properties.getProperty("WanTargetURL");
        if (WAN_ENABLED && WAN_TARGET_URL == null) {
            log.severe("WAN Target URL not found, terminating");
            System.exit(-1);
        }

        WAN_TARGET_NAME = properties.getProperty("WanTargetClusterName");
        if (WAN_ENABLED && WAN_TARGET_NAME == null) {
            log.severe("WAN Target cluster name not found, terminating");
            System.exit(-1);
        }

        JDBC_DRIVER_CLASS = properties.getProperty("JdbcDriverClassName");
        if(JDBC_DRIVER_CLASS == null) {
            log.severe("JDBC driver not configured, terminating");
            System.exit(-1);
        }

        JDBC_PROTOCOL = properties.getProperty("JdbcProtocol");
        if(JDBC_PROTOCOL == null) {
            log.severe("JDBC driver protocol not defined, terminating");
            System.exit(-1);
        }

        JDBC_HOST = properties.getProperty("JdbcHost");
        if(JDBC_HOST == null) {
            log.severe("JDBC host not defined, terminating");
            System.exit(-1);
        }

        JDBC_PORT = properties.getProperty("JdbcPort");
        if(JDBC_PORT == null) {
            log.severe("JDBC port not defined, terminating");
            System.exit(-1);
        }

        JDBC_DB_NAME = properties.getProperty("JdbcDBName");
        if(JDBC_DB_NAME == null) {
            log.severe("JDBC database name not defined, terminating");
            System.exit(-1);
        }

        JDBC_TIMEZONE = properties.getProperty("JdbcTimeZone");
        if(JDBC_TIMEZONE == null) {
            log.warning("JDBC timezone name not defined, using default timezone UTC");
            JDBC_TIMEZONE="UTC";
        }

        JDBC_USER = properties.getProperty("JdbcUserName");
        if(JDBC_USER == null) {
            log.severe("JDBC username name not defined, terminating");
            System.exit(-1);
        }

        JDBC_PASSWORD = properties.getProperty("JdbcPassword");
        if(JDBC_PASSWORD == null) {
            log.severe("JDBC password name not defined, terminating");
            System.exit(-1);
        }

        String auto_commit = properties.getProperty("JdbcAutoCommit");
        if(auto_commit == null)
            JDBC_AUTOCOMMIT = false;
        else
            JDBC_AUTOCOMMIT = Boolean.valueOf(auto_commit);

        String txn_count_prop = properties.getProperty("TransactionCount");
        if(txn_count_prop == null) {
            log.warning("Number of transactions per account for pre-loaded database is not configured, using the default of 5 transactions per account");
            TRANSACTION_COUNT = 5;
        } else
            TRANSACTION_COUNT = Integer.parseInt(txn_count_prop);

        String merch_count_prop = properties.getProperty("MerchantCount");
        if(merch_count_prop == null) {
            log.warning("Number of merchants for pre-loaded database is not configured, creating 151 merchants as default");
            MERCHANT_COUNT = 151;
        } else
            MERCHANT_COUNT = Integer.parseInt(merch_count_prop);

        String acc_count_prop = properties.getProperty("AccountCount");
        if(acc_count_prop == null) {
            log.warning("Number of accounts for pre-loaded database is not configured, creating 2000 accounts as default");
            ACCOUNT_COUNT = 2000;
        } else
            ACCOUNT_COUNT = Integer.parseInt(acc_count_prop);
    }
}
