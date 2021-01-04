package com.hazelcast.certification.util;

import com.hazelcast.certification.database.AccountDB;
import com.hazelcast.certification.database.MerchantDB;
import com.hazelcast.certification.database.TransactionDB;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;

public class GenerateAll {

    private final static ILogger log = Logger.getLogger(GenerateAll.class);

    private static String[] accountIDs;

    public static void main(String[] args) {

        log.info("Cleaning and generating merchants");

        try {
            MerchantDB.initialClean();
            CompletableFuture<Void> merchantFuture = CompletableFuture.runAsync(() -> {
                int c = 0;
                try {
                    c = MerchantDB.generateAndStoreMultiple(FraudDetectionProperties.MERCHANT_COUNT);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                log.info("Generated " + c + " merchants");
            });


            log.info("Cleaning and generating accounts");
            AccountDB.initialClean();

            CompletableFuture<Void> accountFuture = CompletableFuture.runAsync(() -> {
                try {
                    accountIDs = AccountDB.storeAndGetAccountIDs(FraudDetectionProperties.ACCOUNT_COUNT);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                log.info("Generated " + accountIDs.length + " accounts");
            });

            /**
             * Account table must be completed before Transaction data can be created due to dependency
             */
            log.info("Merchant and Account launched, waiting on completion");
            CompletableFuture<Void> merchantAndAccount = CompletableFuture.allOf(merchantFuture, accountFuture);
            try {
                merchantAndAccount.get();
            } catch (Exception e) {
                e.printStackTrace();
            }

            log.info("Cleaning and generating transactions");
            TransactionDB.initialClean();
            CompletableFuture<Void> transactionFuture = CompletableFuture.runAsync(() -> {
                int c = 0;
                try {
                    c = TransactionDB.storeTransactionsForAccounts(accountIDs, FraudDetectionProperties.TRANSACTION_COUNT);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                log.info("Generated " + c + " transactions");
            });

            log.info("Transactions launched, waiting on completion");
            CompletableFuture<Void> remaining = CompletableFuture.allOf(transactionFuture);
            try {
                remaining.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
            log.info("All complete.");
        } catch(SQLException sqlException)  {
            log.severe(sqlException);
        }
        System.exit(0);
    }
}
