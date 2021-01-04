package com.hazelcast.certification.domainstore;

import com.hazelcast.certification.database.MerchantDB;
import com.hazelcast.certification.domain.Merchant;
import com.hazelcast.certification.util.ConnectionUtil;
import com.hazelcast.certification.util.FraudDetectionProperties;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.MapStore;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class MerchantStore implements MapStore<String, Merchant> {

    private final static ILogger log = Logger.getLogger(MerchantStore.class);
    private static final String selectKeysString = "select id from merchant";

    private static final int ID = 1;
    private Connection conn;

    public MerchantStore() {
        conn = ConnectionUtil.getConnection();
    }

    @Override
    public void store(String s, Merchant merchant) {

    }

    @Override
    public void storeAll(Map<String, Merchant> map) {

    }

    @Override
    public void delete(String s) {

    }

    @Override
    public void deleteAll(Collection<String> collection) {

    }

    @Override
    public synchronized com.hazelcast.certification.domain.Merchant load(String s) {
        try {
            return MerchantDB.readFromDatabase(s);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public synchronized Map<String, com.hazelcast.certification.domain.Merchant> loadAll(Collection<String> collection) {
        Map<String, Merchant> results = new HashMap<>(collection.size());
        collection.forEach((String key) -> {
            Merchant m = load(key);
            results.put(key, m);
        });

        return results;
    }

    @Override
    public synchronized Iterable<String> loadAllKeys() {
        int size = FraudDetectionProperties.MERCHANT_COUNT;
        List<String> allKeys = new ArrayList<>(size);
        try {
            Statement statement = conn.createStatement();
            ResultSet rs = statement.executeQuery(selectKeysString);
            while (rs.next()) {
                String merchantID = rs.getString(ID);
                allKeys.add(merchantID);
            }
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        log.info("MapLoader.loadAllKeys() on MERCHANT table returning " + allKeys.size() + " keys");
        return allKeys;
    }
}
