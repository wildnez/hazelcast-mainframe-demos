package com.hazelcast.certification.server;

import com.hazelcast.certification.business.ruleengine.HistoricalDataRuleEngine;
import com.hazelcast.certification.business.ruleengine.MerchantRuleEngine;
import com.hazelcast.certification.business.ruleengine.RulesResult;
import com.hazelcast.certification.domain.Transaction;
import com.hazelcast.certification.util.FraudDetectionProperties;
import com.hazelcast.certification.util.TransactionUtil;
import com.hazelcast.collection.IQueue;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.internal.ascii.rest.RestValue;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.*;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.IMap;

import java.io.Serializable;
import java.util.AbstractCollection;
import java.util.Iterator;

public class FraudDetectionServer implements Serializable {

    private final static ILogger log = Logger.getLogger(FraudDetectionServer.class);

    private static final String TXN_QUEUE_ID = FraudDetectionProperties.TXN_QUEUE_ID;

    private static final String ACCOUNT_MAP = FraudDetectionProperties.ACCOUNT_MAP;
    private static final String MERCHANT_MAP = FraudDetectionProperties.MERCHANT_MAP;
    private static final String RULESRESULT_MAP = FraudDetectionProperties.RULESRESULT_MAP;

    private JetInstance jet;

    private static MerchantRuleEngine merchantRuleEngine;
    private static HistoricalDataRuleEngine historicalRuleEngine;

    public static void main(String[] args) {
        new FraudDetectionServer().start();
    }

    private void init() {
        JetConfig jetConfig = new JetConfig();
        final NetworkConfig networkConfig = new NetworkConfig();

        networkConfig.getInterfaces().setEnabled(false);
        networkConfig.getJoin().getMulticastConfig().setEnabled(false);
        networkConfig.getJoin().getTcpIpConfig().setEnabled(true);
        networkConfig.getRestApiConfig().setEnabled(true).enableAllGroups();

        networkConfig.getJoin().getTcpIpConfig().addMember(FraudDetectionProperties.SERVER_IP+":"+FraudDetectionProperties.SERVER_PORT);
        jetConfig.configureHazelcast(c -> {
            c.setNetworkConfig(networkConfig);
        });
        jetConfig.setHazelcastConfig(ImdgConfigInitializer.getImdgConfigurations());

        jet = Jet.newJetInstance(jetConfig);

        IMap merchantMap = jet.getHazelcastInstance().getMap(MERCHANT_MAP);
        log.info("Total Merchants loaded in Hazelcast: "+merchantMap.size());
        merchantRuleEngine = new MerchantRuleEngine(merchantMap);

        IMap accountMap = jet.getHazelcastInstance().getMap(ACCOUNT_MAP);
        log.info("Total Accounts loaded in Hazelcast: "+accountMap.size());
        historicalRuleEngine = new HistoricalDataRuleEngine(accountMap);
    }

    private void start() {
        init();

        Pipeline p = buildPipeline();

        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("Fraud Detection Job");

        jet.newJobIfAbsent(p, jobConfig);
    }

    private Pipeline buildPipeline() {
        Pipeline p = Pipeline.create();

        StreamStage<Transaction> transaction = p.readFrom(buildQueueSource())
                .withoutTimestamps()
                .map(restValue -> transformToTransaction(restValue))
                .setName("Transform incoming RestValue into Transaction");

        StreamStage<Transaction> txnPostMerchantRules = transaction.map(txn -> applyMerchantRules(txn))
                .setName("Apply Merchant based rules");

        StreamStage<Transaction> txnPostHistoricalRules = txnPostMerchantRules.map(txn -> applyHistoricalTxnRules(txn))
                .setName("Apply Historical transactions rules");

        txnPostHistoricalRules.writeTo(Sinks.map(RULESRESULT_MAP, Transaction::getTransactionId, Transaction::getRulesResult));
        txnPostHistoricalRules.writeTo(buildQueueSink());

        log.info(p.toDotString());

        return p;
    }

    private Sink<? super Transaction> buildQueueSink() {
        return SinkBuilder.sinkBuilder("queueSink",
                jet -> jet.jetInstance().getHazelcastInstance().<String>getQueue("rules_result_string_queue"))
                .<Transaction>receiveFn( (queue, txn)-> queue.add(transformResultsToString(txn)))
                .build();
    }

    private static String transformResultsToString(Transaction txn) {
        RulesResult result = txn.getRulesResult();
        return "txnID: "+txn.getTransactionId()+" "+result.getMerchantRisk()+" "+result.getTransactionRisk();
    }

    private static Transaction applyHistoricalTxnRules(Transaction txn) {
        log.info("Applying rules on historical data");
        historicalRuleEngine.apply(txn);
        return txn;
    }

    private static Transaction applyMerchantRules(Transaction txn) {
        log.info("Applying merchant rules");
        merchantRuleEngine.apply(txn);
        return txn;
    }

    private static Transaction transformToTransaction(RestValue restValue) {
        log.info("Applying transformToTransaction");
        return TransactionUtil.transformToTransaction(new String(restValue.getValue()));
    }

    private StreamSource<RestValue> buildQueueSource() {
        StreamSource<RestValue> source = SourceBuilder.<QueueContext<RestValue>>stream(TXN_QUEUE_ID, c -> new QueueContext<>(c.jetInstance().getHazelcastInstance().getQueue(TXN_QUEUE_ID)))
                .<RestValue>fillBufferFn(QueueContext::fillBuffer)
                .build();

        return source;
    }

    static class QueueContext<T> extends AbstractCollection<T> {
        static final int MAX_ELEMENTS = 1024;
        IQueue<T> queue;
        SourceBuilder.SourceBuffer<T> buf;
        QueueContext(IQueue<T> queue) {
            this.queue = queue;
        }

        void fillBuffer(SourceBuilder.SourceBuffer<T> buf) {
            this.buf = buf;
            queue.drainTo(this, MAX_ELEMENTS);
        }
        @Override
        public boolean add(T item) {
            buf.add(item);
            return true;
        }
        @Override
        public Iterator<T> iterator() {
            throw new UnsupportedOperationException();
        }
        @Override
        public int size() {
            throw new UnsupportedOperationException();
        }
    }
}
