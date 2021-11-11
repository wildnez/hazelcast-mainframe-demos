package com.hazelcast.certification.domainstore;

import com.hazelcast.certification.database.AccountDB;
import com.hazelcast.certification.database.TransactionDB;
import com.hazelcast.certification.domain.Account;
import com.hazelcast.certification.util.ConnectionUtil;
import com.hazelcast.certification.util.MyProperties;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.MapStore;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class AccountStore implements MapStore<String, Account> {

    private final static ILogger log = Logger.getLogger(AccountStore.class);

    private Connection conn;
    private static final String selectKeysString = "select acct_number from account";
    private static final int ACCT_NUMBER = 1;

    public AccountStore() {
        this.conn = ConnectionUtil.getConnection();
    }

    @Override
    public void storeAll(Map map) {

    }

    @Override
    public void deleteAll(Collection collection) {

    }

    @Override
    public void store(String s, Account account) {

    }

    @Override
    public void delete(String s) {

    }


    @Override
    public synchronized Account load(String accountID) {
        Account account = null;
        try {
            account = AccountDB.readFromDatabase(accountID);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        if(account != null)
            account.setHistoricalTransactions(TransactionDB.readTransactionsForAccountFromDB(account.getAccountNumber()));
        return account;
    }

    @Override
    public synchronized Map<String, Account> loadAll(Collection<String> collection) {
        Map<String, Account> results = new HashMap<>(collection.size());
        collection.stream().forEach((String key) -> {
            Account a = load(key);
            results.put(key, a);
        });

        return results;
    }

    @Override
    public synchronized Iterable<String> loadAllKeys() {
        int size = MyProperties.ACCOUNT_COUNT;
        List<String> allKeys = new ArrayList<>(size);
        try {
            Statement statement = conn.createStatement();
            ResultSet rs = statement.executeQuery(selectKeysString);
            while (rs.next()) {
                String accountNum = rs.getString(ACCT_NUMBER);
                allKeys.add(accountNum);
            }
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        log.info("MapLoader.loadAllKeys() on ACCOUNT table returning " + allKeys.size() + " keys");
        return allKeys;
    }
}
