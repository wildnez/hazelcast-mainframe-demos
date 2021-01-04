package com.hazelcast.certification.domain;

import com.hazelcast.certification.business.ruleengine.RulesResult;

import java.io.Serializable;

/**
 * Object representation of incoming and historical transaction
 *
 */
public class Transaction implements Serializable {

	private String transactionId;

	private String acctNumber;
	private String merchantId;
	private Double amount = 0.0;

	private String location;
	private int fraudResult = -1;

	private Boolean paymentResult = Boolean.TRUE;
	private long timeStamp;

	private String countryCode;
	private String responseCode;
	private String txnCurrency;

	private String txnCode = "";
	private String txnCity;

	private RulesResult rulesResult;

	// No-arg constructor for DataSerialization

	public Transaction() {

	}

	public Transaction(String txnId) {
		this.transactionId = txnId;
		rulesResult = new RulesResult(transactionId);
	}

	public RulesResult getRulesResult() {
		return rulesResult;
	}

	public void setRulesResult(RulesResult rulesResult) {
		this.rulesResult = rulesResult;
	}

	public String getTransactionId() {
		return transactionId;
	}

	public void setTransactionId(String transactionId) {
		this.transactionId = transactionId;
	}

	public long getTimeStamp() {
		return timeStamp;
	}

	public void setTimeStamp(long timeStamp) {
		this.timeStamp = timeStamp;
	}

	public String getCountryCode() {
		return countryCode;
	}

	public void setCountryCode(String countryCode) {
		this.countryCode = countryCode;
	}

	public String getResponseCode() {
		return responseCode;
	}

	public void setResponseCode(String responseCode) {
		this.responseCode = responseCode;
	}

	public String getTxnCurrency() {
		return txnCurrency;
	}

	public void setTxnCurrency(String txnCurrency) {
		this.txnCurrency = txnCurrency;
	}

	public String getTxnCode() {
		return txnCode;
	}

	public void setTxnCode(String txnCode) {
		this.txnCode = txnCode;
	}

	public String getTxnCity() {
		return txnCity;
	}

	public void setTxnCity(String txnCity) {
		this.txnCity = txnCity;
	}

	public void setAccountNumber(String acct) { this.acctNumber = acct; }
	public String getAccountNumber() {
		return acctNumber;
	}

	public void setAmount(Double amt) { this.amount = amt; }
	public Double getAmount() {
		return amount;
	}

	public void setMerchantId(String id) {
		this.merchantId = id;
	}
	public String getMerchantId() { return merchantId; }

	public void setLocation(String l) { this.location = l; }
	public String getLocation() { return location; }


	@Override
	public String toString() {
		return "TransactionDB " + transactionId + " account " + acctNumber + " merchant " + merchantId + " amount " + amount;
	}

}
