package com.hazelcast.certification.database;

import com.hazelcast.certification.domain.Account;
import com.hazelcast.certification.util.ConnectionUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;

public class AccountDB {

    private final static ILogger log = Logger.getLogger(AccountDB.class);

    private static java.sql.Connection conn;

    private static final DecimalFormat accountFormat  = new DecimalFormat( "0000000000000000");    // 16 digit

    // Index positions
    private static final int ACCT_NUMBER = 1;
    private static final int CREDIT_LIMIT = 2;
    private static final int BALANCE = 3;
    private static final int ACCT_STATUS = 4;
    private static final int LOCATION = 5;

    private static final String createTableString =
            "create table account ( " +
                    "acct_number    char(16)     not null, " +
                    "credit_limit   float, " +
                    "balance        float, " +
                    "acct_status    smallint, " +
                    "location       varchar(10), " +
                    "primary key (acct_number) " +
                    ")";

    private static final String insertTemplate =
            "insert into account (acct_number, credit_limit, balance, acct_status, location) " +
                    " values (?, ?, ?, ?, ?)";

    private static final String selectTemplate =
            "select acct_number, credit_limit, balance, acct_status, location from account where acct_number = ?";

    static {
        conn = ConnectionUtil.getConnection();
    }

    private AccountDB(){}

    public static void initialClean()  {
        try {
            PreparedStatement createStatement = conn.prepareStatement("delete from account");
            createStatement.executeUpdate();
            createStatement.close();
            log.info("AccountDB table cleaned");
        } catch (SQLException se) {
            se.printStackTrace();
            System.exit(-1);
        }
    }

    private static com.hazelcast.certification.domain.Account generate(int id) {
        try {
            return new com.hazelcast.certification.domain.Account(accountFormat.format(id));
        } catch (Throwable t) {
            t.printStackTrace();
            System.exit(-1);
            return null;
        }
    }

    public static String[] storeAndGetAccountIDs(int count) throws SQLException {
        String[] accountIDs = new String[count];
        for (int i = 0; i < count; i++) {
            Account a = generate(i);
            accountIDs[i] = a.getAccountNumber();
            writeToDatabase(a);
        }
        return accountIDs;
    }

    private static void writeToDatabase(Account a) throws SQLException {
        PreparedStatement insertStatement = conn.prepareStatement(insertTemplate);
        try {
           // log.info("Writing to database for account id "+a.getAccountNumber());
            insertStatement.setString(ACCT_NUMBER, a.getAccountNumber());
            insertStatement.setDouble(CREDIT_LIMIT, a.getCreditLimit());
            insertStatement.setDouble(BALANCE, a.getBalance());
            insertStatement.setInt(ACCT_STATUS, a.getAccountStatus().ordinal());
            insertStatement.setString(LOCATION, a.getLastReportedLocation());
            insertStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(-1);
        } finally {
            conn.commit();
            insertStatement.close();
        }

    }

    public static synchronized Account readFromDatabase(String id) throws SQLException {
        if (id == null) {
            log.warning("AccountDB.readTransactionsForAccountFromDB(): Passed null id, returning null");
            return null;
        }
        PreparedStatement selectStatement = conn.prepareStatement(selectTemplate);
        try {
            selectStatement.setString(ACCT_NUMBER, id);
            ResultSet rs = selectStatement.executeQuery();
            Account a = new Account();
            if (rs == null) {
                log.warning("AccountDB.readTransactionsForAccountFromDB(): Null resultSet trying to read AccountDB " + id);
                return null;
            }
            if (rs.next()) {
                a.setAccountNumber(rs.getString(ACCT_NUMBER));
                a.setCreditLimit(rs.getDouble(CREDIT_LIMIT));
                a.setBalance(rs.getDouble(BALANCE));
                int statusValue = rs.getInt(ACCT_STATUS);
                a.setAccountStatus(com.hazelcast.certification.domain.Account.AccountStatus.values()[statusValue]);
                a.setLastReportedLocation(rs.getString(LOCATION));
            }
            return a;
        } catch (SQLException e) {
            log.info(e.getMessage());
            return null;
        } finally {
            selectStatement.close();
        }
    }
}
