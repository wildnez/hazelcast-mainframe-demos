package com.hazelcast.certification.util;

import com.hazelcast.certification.business.ruleengine.RulesResult;
import com.hazelcast.certification.domain.Transaction;
import org.joda.time.DateTime;

import java.io.Serializable;
import java.util.Random;

public class TransactionUtil implements Serializable {

	private static Random countryCodeRandom;
	private static Random cityCodeRandom;
	private static Random responseCodeRandom;

	private TransactionUtil() {}

	static {
		countryCodeRandom = new Random(1);
		cityCodeRandom = new Random(1);
		responseCodeRandom = new Random(10);
	}

	// last 10 days
	public static long generateTimeStamp() {
		long offset = DateTime.now().getMillis();
		long end = DateTime.now().minusDays(10).getMillis();
		long diff = end - offset + 1;
		return offset + (long) (Math.random() * diff);
	}

	public static String generateTxnCode(int temp) {
		if (temp < 10)
			return "0000" + temp;
		if (temp > 10 && temp < 100)
			return "000" + temp;
		if (temp == 100)
			return "00" + temp;
		return String.valueOf(temp);
	}

	// 001-200
	public static String generateCountryCode() {
		int number = countryCodeRandom.nextInt(200);
		if (number < 10)
			return "00" + number;
		if (number > 10 && number < 100)
			return "0" + number;
		return String.valueOf(number);
	}

	// 95% 00 else random 2-bits
	public static String generateResponseCode(int count) {
		if (count % 95 == 0)
			return String.valueOf(responseCodeRandom.nextInt(20));

		return "00";
	}

	// 00001-10000
	public static String generateCityCode() {
		int temp = cityCodeRandom.nextInt(10000);
		if (temp < 10)
			return "0000" + temp;
		if (temp > 10 && temp < 100)
			return "000" + temp;
		if (temp > 100 && temp < 1000)
			return "00" + temp;
		if (temp > 1000 && temp < 10000)
			return "0" + temp;
		return String.valueOf(temp);
	}

	public static Transaction transformToTransaction(String txnString) {
		Transaction txn = new Transaction();
		String[] split = txnString.split(",");
		txn.setTransactionId(split[0]);
		txn.setAccountNumber(split[1]);
		txn.setMerchantId(split[2]);
		txn.setAmount(Double.valueOf(split[3]));
		txn.setLocation(split[4]);
		txn.setCountryCode(split[5]);
		txn.setResponseCode(split[6]);
		txn.setTxnCurrency(split[7]);
		txn.setTxnCode(split[8]);
		txn.setTxnCity(split[9]);
		txn.setTimeStamp(Long.parseLong(split[10]));
		txn.setRulesResult(new RulesResult(txn.getTransactionId()));
		return txn;
	}
}
