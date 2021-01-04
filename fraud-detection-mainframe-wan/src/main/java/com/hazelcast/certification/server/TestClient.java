package com.hazelcast.certification.server;

import com.hazelcast.certification.business.ruleengine.RulesResult;
import com.hazelcast.certification.database.MerchantDB;
import com.hazelcast.certification.domain.Account;
import com.hazelcast.certification.domain.Merchant;
import com.hazelcast.certification.domain.Transaction;
import com.hazelcast.certification.util.FraudDetectionProperties;
import com.hazelcast.certification.util.TransactionUtil;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.collection.IQueue;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.ascii.rest.RestValue;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetClientConfig;
import com.hazelcast.map.IMap;

import java.io.IOException;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.Random;

public class TestClient {
    public static void main(String[] args) throws SQLException, IOException {
        //testJetQueueOffer();
        testJetGet();
        //testImdg();
    }

    private static byte[] getCommaSeparatedTxnString(Transaction t) {
        String data = t.getTransactionId()+ ","+ t.getAccountNumber()+","+t.getMerchantId()+","+t.getAmount()
                +","+t.getLocation()+","+t.getCountryCode()+","+t.getResponseCode()+","+t.getTxnCurrency()
                +","+t.getTxnCode()+","+t.getTxnCity()+","+t.getTimeStamp();
        System.out.println("--> "+data);
        return data.getBytes();
    }

    private static Merchant getMerchant(String merchantId) throws SQLException {
        return MerchantDB.readFromDatabase(merchantId);
    }

    private static void testJetQueueOffer() throws SQLException {
        DecimalFormat accountFormat  = new DecimalFormat( "0000000000000000");
        DecimalFormat txnFormat      = new DecimalFormat("00000000000000");
        DecimalFormat merchantFormat = new DecimalFormat("0000");       // 4 digit
        Random merchantRandom = new Random(1);;

        JetClientConfig config = new JetClientConfig();
        config.getNetworkConfig().addAddress(FraudDetectionProperties.SERVER_IP+":"+FraudDetectionProperties.SERVER_PORT);
        config.setClusterName("dev");
        JetInstance client = Jet.newJetClient(config);
        IQueue queue = client.getHazelcastInstance().getQueue(FraudDetectionProperties.TXN_QUEUE_ID);

        for(int i=0; i<100; i++) {
            Account a = new Account(accountFormat.format(i));
            String accountID = a.getAccountNumber();
            Transaction t = new Transaction(accountID + txnFormat.format(i));

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

            queue.offer(new RestValue(getCommaSeparatedTxnString(t), "String".getBytes()));
        }
    }

    private static void testJetGet() {
        JetClientConfig config = new JetClientConfig();
        config.getNetworkConfig().addAddress("169.45.220.174:5701");
        config.setClusterName("openshift");
        JetInstance client = Jet.newJetClient(config);
        IMap<String, RulesResult> resultMap = client.getMap("rulesresult");
        for(String txnID : resultMap.keySet()) {
            RulesResult result = resultMap.get(txnID);
            System.out.println("txnID: "+txnID+" "+result.getMerchantRisk()+" "+result.getTransactionRisk() );
        }
    }

    private static void testJetQueue() {
        JetClientConfig config = new JetClientConfig();
        config.setClusterName("dev");
        JetInstance client = Jet.newJetClient(config);
        IQueue<String> results = client.getHazelcastInstance().getQueue("rules_result_string_queue");
        for(String txnID : results) {
            System.out.println(txnID);
        }
    }

    private static void testImdg() {
        HazelcastInstance hazelcast = HazelcastClient.newHazelcastClient();
        IMap<String, Account> acct_map = hazelcast.getMap("account");
        System.out.println("Account map size: "+acct_map.size());

        IMap<String, Merchant> merchant_map = hazelcast.getMap("merchant");
        System.out.println("Account map size: "+merchant_map.size());

        acct_map.forEach((k, v) -> {
            System.out.println("Account ID: "+k+ " and total transactions: "+v.getHistoricalTransactions().size());
        });

        merchant_map.forEach((k, v) -> {
            System.out.println("Merchant: "+k+" "+v.getMerchantName());
        });

        hazelcast.shutdown();

    }
}
