package com.hazelcast.certification.database;

import com.hazelcast.certification.util.ConnectionUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;

public class MerchantDB {

    private final static ILogger log = Logger.getLogger(MerchantDB.class);
    private static java.sql.Connection conn;


    private static final DecimalFormat merchantFormat = new DecimalFormat("0000");       // 4 digit

    // Index positions
    private static final int ID = 1;
    private static final int NAME = 2;
    private static final int REPUTATION = 3;
    private static final int AVG_TXN_AMOUNT = 4;
    private static final int LOCATION = 5;

    private static final String createTableString =
            "create table merchant ( " +
                    "id             char(8)     not null, " +
                    "name           varchar(32), " +
                    "reputation     smallint, " +
                    "avg_txn_amount float, " +
                    "location       varchar(10), " +
                    "primary key (id) " +
                    ")";

    private static final String insertTemplate =
            "insert into merchant (id, name, reputation, avg_txn_amount, location) " +
                    " values (?, ?, ?, ?, ?)";

    private static final String selectTemplate =
            "select id, name, reputation, avg_txn_amount, location from merchant where id = ?";


    static {
        conn = ConnectionUtil.getConnection();
    }

    private MerchantDB(){}

    private static com.hazelcast.certification.domain.Merchant generate(int id) {
        try {
            return new com.hazelcast.certification.domain.Merchant(merchantFormat.format(id));
        } catch (Throwable t) {
            t.printStackTrace();
            System.exit(-1);
            return null;
        }
    }

    public static int generateAndStoreMultiple(int count) throws SQLException {
        for (int i = 0; i < count; i++) {
            com.hazelcast.certification.domain.Merchant m = generate(i);
            writeToDatabase(m);
        }
        return count;
    }

    public static void initialClean() throws SQLException {
            //PreparedStatement createStatement = conn.prepareStatement(createTableString);
        PreparedStatement createStatement = conn.prepareStatement("DELETE from merchant");
        try {
            createStatement.executeUpdate();
            createStatement.close();
            log.info("MerchantDB table cleaned");
        } catch (SQLException se) {
            se.printStackTrace();
            System.exit(-1);
        } finally {
            createStatement.close();
        }

    }

    public static synchronized void writeToDatabase(com.hazelcast.certification.domain.Merchant m) throws SQLException {
        PreparedStatement insertStatement = conn.prepareStatement(insertTemplate);
        try {
            insertStatement.setString(ID, m.getMerchantId());
            insertStatement.setString(NAME, m.getMerchantName());
            insertStatement.setInt(REPUTATION, m.getReputation());
            insertStatement.setDouble(AVG_TXN_AMOUNT, m.getAvgTxnAmount());
            insertStatement.setString(LOCATION, m.getLocation());
            insertStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(-1);
        } finally {
            insertStatement.close();
        }
    }

    public static synchronized com.hazelcast.certification.domain.Merchant readFromDatabase(String id) throws SQLException {
        if (id == null) {
            return null;
        }
        PreparedStatement selectStatement = conn.prepareStatement(selectTemplate);
        try {
            selectStatement.setString(ID, id);
            ResultSet rs = selectStatement.executeQuery();
            com.hazelcast.certification.domain.Merchant m = new com.hazelcast.certification.domain.Merchant();
            if (rs == null) {
                log.warning("MerchantDB.readTransactionsForAccountFromDB(): Null resultSet trying to read MerchantDB " + id);
                return null;
            }
            while (rs.next()) {
                m.setMerchantID(rs.getString(ID));
                m.setMerchantName(rs.getString(NAME));
                m.setReputation(rs.getInt(REPUTATION));
                m.setAvgTxnAmount(rs.getDouble(AVG_TXN_AMOUNT));
                m.setLocation(rs.getString(LOCATION));
            }
            return m;
        } catch (SQLException e) {
            log.severe(e.getMessage());
            return null;
        } finally {
            selectStatement.close();
        }
    }
}
