package com.hazelcast.certification.domain;

import com.hazelcast.certification.domain.internal.TransactionQueue;
import com.hazelcast.certification.util.FraudDetectionProperties;

import java.io.Serializable;

public class Account implements Serializable {

    public enum AccountStatus { CURRENT, OVERDUE, CLOSED }

    protected String accountNumber = "*invalid*";
    private Double creditLimit = 1000.0;
    private Double balance = 0.0;
    private AccountStatus status = AccountStatus.CURRENT;
    private String lastReportedLocation = "unknown";

    private TransactionQueue<Transaction> historicalTransactions = new TransactionQueue<Transaction>(FraudDetectionProperties.TRANSACTION_COUNT);

    public Account(String accountNumber) {
        this.accountNumber = accountNumber;
    }

    // no arg constructor needed for DataSerialization
    public Account() {}

    public String getAccountNumber() { return accountNumber; }
    public void setAccountNumber(String acctNo) { this.accountNumber = acctNo; }

    public void setBalance(Double balance) { this.balance = balance; }
    public Double getBalance() {
        return balance;
    }
    public void adjustBalance(Double adjustment) { this.balance += balance; }

    public void setCreditLimit(Double limit) { this.creditLimit = limit; }
    public Double getCreditLimit() {
        return creditLimit;
    }

    public void setAccountStatus(AccountStatus status) { this.status = status; }
    public AccountStatus getAccountStatus() { return status; }

    public void setLastReportedLocation(String location) { this.lastReportedLocation = location; }
    public String getLastReportedLocation() { return lastReportedLocation; }

    public TransactionQueue<Transaction> getHistoricalTransactions() {
        return historicalTransactions;
    }

    public void setHistoricalTransactions(TransactionQueue<Transaction> historicalTransactions) {
        this.historicalTransactions = historicalTransactions;
    }

    public String toString() {
        return "Acct " + accountNumber + " " + creditLimit + " " + balance + " " + status;
    }


}
