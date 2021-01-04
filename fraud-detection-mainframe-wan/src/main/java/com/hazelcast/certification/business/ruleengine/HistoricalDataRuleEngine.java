package com.hazelcast.certification.business.ruleengine;

import com.hazelcast.certification.domain.Account;
import com.hazelcast.certification.domain.Transaction;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.IMap;

import java.util.List;

public class HistoricalDataRuleEngine {

	private final static ILogger log = Logger.getLogger(HistoricalDataRuleEngine.class);

	private IMap<String, Account> accountMap;

	public HistoricalDataRuleEngine(IMap accountMap) {
		this.accountMap = accountMap;
	}


	public RulesResult apply(Transaction currentTxn) {
		Double avgTxnAmount = 0.0;
		RulesResult result = currentTxn.getRulesResult();
		List<Transaction> historicalTxns = accountMap.get(currentTxn.getAccountNumber()).getHistoricalTransactions();
		for(Transaction txn : historicalTxns) {
			System.out.println(txn.toString());
			avgTxnAmount += txn.getAmount();
		}

		avgTxnAmount = avgTxnAmount/historicalTxns.size();
		Double amount = currentTxn.getAmount();

		int stddev = (int) (avgTxnAmount / 5);
		// Roughly 70% of transactions should be within 1 std deviation
		if (amount >= avgTxnAmount-stddev && amount <= avgTxnAmount+stddev) {
			result.setTransactionRisk(TransactionRisk.LOW);
		}
		// Roughly 95% of transactions should be within 2 std deviations
		else if (amount >= avgTxnAmount-2*stddev && amount <= avgTxnAmount+2*stddev) {
			result.setTransactionRisk(TransactionRisk.MEDIUM);
		}
		// Over 99% of transactions should be within 3 - currently treating everything
		// outside of 2 std deviations the same
		else {
			result.setTransactionRisk(TransactionRisk.HIGH);
		}
		return result;
	}
}
