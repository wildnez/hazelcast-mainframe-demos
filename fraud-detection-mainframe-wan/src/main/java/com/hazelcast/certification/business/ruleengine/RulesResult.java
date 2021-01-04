package com.hazelcast.certification.business.ruleengine;

import java.io.Serializable;

public class RulesResult implements Serializable {

    private static final long serialVersionUID = 4L;

    private String transactionID;
    private MerchantRisk merchantRisk;
    private TransactionRisk transactionRisk;

    public RulesResult(String transactionID) {
        this.transactionID = transactionID;
    }

    public void setMerchantRisk(MerchantRisk risk) {
        this.merchantRisk = risk;
    }

    public MerchantRisk getMerchantRisk() {
        return merchantRisk;
    }

    public String getTransactionID() {
        return transactionID;
    }

    public TransactionRisk getTransactionRisk() {
        return transactionRisk;
    }

    public void setTransactionRisk(TransactionRisk transactionRisk) {
        this.transactionRisk = transactionRisk;
    }

}


