package com.hazelcast.certification.util;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static com.hazelcast.certification.util.FraudDetectionProperties.*;

public class ConnectionUtil {

    private final static ILogger log = Logger.getLogger(ConnectionUtil.class);

    //private final static String dbUrl = "jdbc:mysql://localhost:3306/fraud_db?&serverTimezone=UTC";
    private final static String dbUrl = JDBC_PROTOCOL+"://"+JDBC_HOST+":"+JDBC_PORT+"/"+JDBC_DB_NAME+"?&serverTimezone="+JDBC_TIMEZONE;

    private final static String user = JDBC_USER;
    private final static String password = JDBC_PASSWORD;

    private static java.sql.Connection conn;

    private ConnectionUtil() {}

    public static Connection getConnection()  {
        if(conn == null) {
            synchronized (ConnectionUtil.class) {
                if(conn == null) {
                    try {
                        String driverClassName = JDBC_DRIVER_CLASS;
                        log.info( "Loading MySql JDBC Driver: " + driverClassName );
                        Class.forName( driverClassName );

                        log.info( "Establishing connection with database at: " + dbUrl );
                        conn = DriverManager.getConnection( dbUrl, user, password );
                        System.out.println( "  successful connect" );
                        conn.setAutoCommit(JDBC_AUTOCOMMIT);

                    } catch (SQLException | ClassNotFoundException e) {
                        e.printStackTrace();
                        System.exit(-1);
                    }
                }
            }
        }
        return conn;
    }
}
