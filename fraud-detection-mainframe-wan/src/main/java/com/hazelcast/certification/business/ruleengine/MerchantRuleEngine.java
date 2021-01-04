package com.hazelcast.certification.business.ruleengine;

import com.hazelcast.certification.domain.Merchant;
import com.hazelcast.certification.domain.Transaction;
import com.hazelcast.map.IMap;

public class MerchantRuleEngine {

    private IMap<String, Merchant> merchantMap;

    public MerchantRuleEngine(IMap merchantMap) {
        this.merchantMap = merchantMap;
    }

    public RulesResult apply(Transaction transaction) {
        String merchantID = transaction.getMerchantId();
        Merchant merchant = merchantMap.get(merchantID);
        Double avgTxnAmount = merchant.getAvgTxnAmount();
        Double amount = transaction.getAmount();
        Merchant.RISK risk;

        RulesResult result = transaction.getRulesResult();

        int stddev = (int) (avgTxnAmount / 5);
        // Roughly 70% of transactions should be within 1 std deviation
        if (amount >= avgTxnAmount-stddev && amount <= avgTxnAmount+stddev) {
            result.setMerchantRisk(MerchantRisk.LOW);
        }
            // Roughly 95% of transactions should be within 2 std deviations
        else if (amount >= avgTxnAmount-2*stddev && amount <= avgTxnAmount+2*stddev) {
            result.setMerchantRisk(MerchantRisk.MEDIUM);
        }
            // Over 99% of transactions should be within 3 - currently treating everything
            // outside of 2 std deviations the same
        else {
            result.setMerchantRisk(MerchantRisk.HIGH);
        }

        return result;
    }
}
