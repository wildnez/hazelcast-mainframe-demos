package com.hazelcast.certification.process.impl.executorService;

import com.hazelcast.certification.business.ruleengine.RulesResult;
import com.hazelcast.certification.domain.Transaction;
import com.hazelcast.certification.process.impl.executorService.data.DataAccessManager;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;

public class FraudDetectionTask implements DataSerializable, HazelcastInstanceAware, Callable<RulesResult> {

	private transient HazelcastInstance hazelcast;

	private Transaction txn;

	public FraudDetectionTask() {}

	void setTransaction(Transaction txn) {
		this.txn = txn;
	}
	
	public RulesResult call() throws Exception {

		DataAccessManager dm = new DataAccessManager();
		dm.setHazelcastInstance(hazelcast);
		List<Transaction> allTxns = dm.updateAndGet(txn);
		
//		HistoricalDataRuleEngine historicalDataRuleEngine = new HistoricalDataRuleEngine(txn, allTxns);
//		historicalDataRuleEngine.executeRules();
//
//		RulesResult rulesResult = new RulesResult();
//		rulesResult.setAccountNumber(txn.getAccountNumber());
//		rulesResult.setTxnStatus(historicalDataRuleEngine.isFraudTxn());

//		return rulesResult;
		return null;
	}

	public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
		this.hazelcast = hazelcastInstance;
	}

	public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
		objectDataOutput.writeObject(txn);
	}

	public void readData(ObjectDataInput objectDataInput) throws IOException {
		txn = objectDataInput.readObject();
	}
}
