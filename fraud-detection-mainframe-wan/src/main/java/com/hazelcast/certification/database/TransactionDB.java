package com.hazelcast.certification.database;

import com.hazelcast.certification.domain.Merchant;
import com.hazelcast.certification.domain.Transaction;
import com.hazelcast.certification.domain.internal.TransactionQueue;
import com.hazelcast.certification.util.ConnectionUtil;
import com.hazelcast.certification.util.MyProperties;
import com.hazelcast.certification.util.TransactionUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class TransactionDB {

    private final static ILogger log = Logger.getLogger(TransactionDB.class);

    private static Connection conn;

    private static final DecimalFormat merchantFormat = new DecimalFormat("0000");       // 4 digit
    private static final DecimalFormat txnFormat      = new DecimalFormat("00000000000000"); // 14 digit

    // Index positions
    private static final int ID = 1;
    private static final int ACCT_NUMBER = 2;
    private static final int MERCHANT_ID = 3;
    private static final int AMOUNT = 4;
    private static final int LOCATION = 5;
    private static final int COUNTRY_CODE = 6;
    private static final int RESPONSE_CODE = 7;
    private static final int CURRENCY = 8;
    private static final int TRANSACTION_CODE = 9;
    private static final int CITY = 10;
    private static final int TIMESTAMP = 11;

    private static final String createTableString =
            "create table transaction ( " +
                    "id             varchar(32)     not null, " +
                    "acct_number    varchar(16), " +         // foreign key but not marking as such
                    "merchant_id    varchar(8), " +          // foreign key but not marking as such
                    "amount         float, " +
                    "location       varchar(10), " +

                    "country_code       varchar(10), " +
                    "response_code       varchar(10), " +
                    "currency       varchar(10), " +
                    "transaction_code       varchar(10), " +
                    "city       varchar(10), " +
                    "timestamp       BIGINT, " +

                    "primary key (id) " +
                    ")";


    private static final String insertTemplate =
            "insert into transaction (id, acct_number, merchant_id, amount, location, country_code, response_code, currency, transaction_code, city, timestamp) " +
                    " values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String selectTransactionForAccountTemplate =
            "SELECT * from transaction where acct_number = ?";

    private static final String selectAccountIDTemplate =
            "SELECT acct_number from account";

    private static final String selectKeysString = "select id from transaction";

    private static Random merchantRandom = new Random(1);

    static {
        conn = ConnectionUtil.getConnection();
    }

    private TransactionDB() {

    }

    public static void initialClean()  {
        try {
            //PreparedStatement createStatement = conn.prepareStatement(createTableString);
            PreparedStatement createStatement = conn.prepareStatement("DELETE from transaction");
            createStatement.executeUpdate();
            createStatement.close();
            log.info("TransactionDB table cleaned");
        } catch (SQLException se) {
            se.printStackTrace();
            System.exit(-1);
        }
    }

    public static int storeTransactionsForAccounts(String[] accountIDs, int transactionsPerAccount) throws SQLException {
        AtomicInteger txnCounter = new AtomicInteger();

        for(String accountID : accountIDs) {

            for(int i=0; i<transactionsPerAccount; i++) {
                Transaction t = new Transaction(accountID+txnFormat.format(i));

                t.setAccountNumber(accountID);

                int merchantNum = merchantRandom.nextInt(151);
                String merchantId = merchantFormat.format(merchantNum);
                t.setMerchantId(merchantId);
                Merchant merchant = getMerchant(merchantId);//MerchantDB.load(merchantId);

                Double txnAmount = merchant.getRandomTransactionAmount(); // Distributed normally around avg txn amount
                t.setAmount(txnAmount);
                t.setLocation("");
                String country_currency_code = TransactionUtil.generateCountryCode();
                t.setCountryCode(country_currency_code);
                t.setResponseCode(TransactionUtil.generateResponseCode(i));
                t.setTxnCurrency(country_currency_code);
                t.setTxnCode(TransactionUtil.generateTxnCode(i));
                t.setTxnCity(TransactionUtil.generateCityCode());
                t.setTimeStamp(TransactionUtil.generateTimeStamp());

                writeToDatabase(t);

                txnCounter.getAndIncrement();
            }
        };

        return txnCounter.intValue();
    }

    private static Merchant getMerchant(String merchantId) throws SQLException {
        return MerchantDB.readFromDatabase(merchantId);
    }

    private static List<String> getAllAccountIDs() {

        System.out.println("Fetching all accounts from the database");

        List<String> accountIDs = new ArrayList<>();
        try {
            PreparedStatement selectStatement = conn.prepareStatement(selectAccountIDTemplate);
            ResultSet rs = selectStatement.executeQuery();
            while(rs.next()) {
                accountIDs.add(rs.getString(1));
            }
            System.out.println("Total AccountDB IDs fetched from the database: "+accountIDs.size());
            selectStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return accountIDs;
    }

    private static synchronized void writeToDatabase(Transaction t) throws SQLException {
        PreparedStatement insertStatement = conn.prepareStatement(insertTemplate);
        try {
            insertStatement.setString(ID, t.getTransactionId());
            insertStatement.setString(ACCT_NUMBER, t.getAccountNumber());
            insertStatement.setString(MERCHANT_ID, t.getMerchantId());
            insertStatement.setDouble(AMOUNT, t.getAmount());
            insertStatement.setString(LOCATION, t.getLocation());
            insertStatement.setString(COUNTRY_CODE, t.getCountryCode());
            insertStatement.setString(RESPONSE_CODE, t.getResponseCode());
            insertStatement.setString(CURRENCY, t.getTxnCurrency());
            insertStatement.setString(TRANSACTION_CODE, t.getTxnCode());
            insertStatement.setString(CITY, t.getTxnCity());
            insertStatement.setLong(TIMESTAMP, t.getTimeStamp());

            insertStatement.executeUpdate();

        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(-1);
        } finally {
            conn.commit();
            insertStatement.close();
        }
    }

    public static synchronized TransactionQueue<Transaction> readTransactionsForAccountFromDB(String accountID) {
        try {
            TransactionQueue queue = new TransactionQueue(MyProperties.TRANSACTION_COUNT);
            PreparedStatement selectStatement = conn.prepareStatement(selectTransactionForAccountTemplate);
            selectStatement.setString(ID, accountID);
            ResultSet rs = selectStatement.executeQuery();
            if (rs == null) {
                log.warning("TransactionDB.readTransactionsForAccountFromDB: no entry for account id " + accountID);
                return null;
            }

            while (rs.next()) {
                Transaction t = new Transaction();
                t.setTransactionId(rs.getString(ID));
                t.setAccountNumber(rs.getString(ACCT_NUMBER));
                t.setMerchantId(rs.getString(MERCHANT_ID));
                t.setAmount(rs.getDouble(AMOUNT));
                t.setLocation(rs.getString(LOCATION));
                t.setCountryCode(rs.getString(COUNTRY_CODE));
                t.setResponseCode(rs.getString(RESPONSE_CODE));
                t.setTxnCurrency(rs.getString(CURRENCY));
                t.setTxnCode(rs.getString(TRANSACTION_CODE));
                t.setTxnCity(rs.getString(CITY));
                t.setTimeStamp(rs.getLong(TIMESTAMP));
                queue.add(t);
            }
            selectStatement.close();
            return queue;
        } catch (SQLException e) {
            log.severe(e.getMessage());
            return null;
        } catch (Throwable t) {
            t.printStackTrace();
            return null;
        }

    }
}
