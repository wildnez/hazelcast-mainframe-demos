package com.hazelcast.certification.server;

import com.hazelcast.certification.business.ruleengine.HistoricalDataRuleEngine;
import com.hazelcast.certification.business.ruleengine.MerchantRuleEngine;
import com.hazelcast.certification.business.ruleengine.RulesResult;
import com.hazelcast.certification.domain.Transaction;
import com.hazelcast.certification.util.MyProperties;
import com.hazelcast.certification.util.License;
import com.hazelcast.certification.util.TransactionUtil;
import com.hazelcast.collection.IQueue;
import com.hazelcast.config.Config;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.ascii.rest.RestValue;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.*;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.IMap;

import java.io.Serializable;
import java.util.AbstractCollection;
import java.util.Iterator;

public class Server implements Serializable {

    private final static ILogger log = Logger.getLogger(Server.class);

    private static final String TXN_QUEUE_ID = MyProperties.TXN_QUEUE_ID;

    private static final String ACCOUNT_MAP = MyProperties.ACCOUNT_MAP;
    private static final String MERCHANT_MAP = MyProperties.MERCHANT_MAP;
    private static final String RULESRESULT_MAP = MyProperties.RULESRESULT_MAP;

    private HazelcastInstance hazelcast;

    private static MerchantRuleEngine merchantRuleEngine;
    private static HistoricalDataRuleEngine historicalRuleEngine;

    public static void main(String[] args) {
        new Server().start();
    }

    private void init() {
        Config config = new Config();
        config.getJetConfig().setEnabled(true);

        config.setLicenseKey(License.KEY);
        NetworkConfig networkConfig = new NetworkConfig();

        networkConfig.getInterfaces().setEnabled(false);
        networkConfig.getJoin().getMulticastConfig().setEnabled(false);
        networkConfig.getJoin().getTcpIpConfig().setEnabled(true);
        networkConfig.getRestApiConfig().setEnabled(true).enableAllGroups();

        networkConfig.getJoin().getTcpIpConfig().addMember(MyProperties.SERVER_IP+":"+MyProperties.SERVER_PORT);
        config.setNetworkConfig(networkConfig);

        MapConfigFactory.updateWithMapConfig(config);

        hazelcast = Hazelcast.newHazelcastInstance(config);

        IMap merchantMap = hazelcast.getMap(MERCHANT_MAP);
        log.info("Total Merchants loaded in Hazelcast: "+merchantMap.size());
        merchantRuleEngine = new MerchantRuleEngine(merchantMap);

        IMap accountMap = hazelcast.getMap(ACCOUNT_MAP);
        log.info("Total Accounts loaded in Hazelcast: "+accountMap.size());
        historicalRuleEngine = new HistoricalDataRuleEngine(accountMap);
    }

    private void start() {
        init();

        Pipeline p = buildPipeline();

        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("Transaction Processing Job");

        JetService jet = hazelcast.getJet();
        jet.newJobIfAbsent(p, jobConfig);
    }

    private Pipeline buildPipeline() {
        Pipeline p = Pipeline.create();

        StreamStage<Transaction> transaction = p.readFrom(buildQueueSource())
                .withoutTimestamps()
                .rebalance()
                .map(restValue -> transformToTransaction(restValue))
                .setName("Transform incoming raw transaction");

        StreamStage<Transaction> txnPostMerchantRules = transaction.map(txn -> applyMerchantRules(txn))
                .setName("Apply Merchant rules");

        StreamStage<Transaction> txnPostHistoricalRules = txnPostMerchantRules.map(txn -> applyHistoricalTxnRules(txn))
                .setName("Apply Historical rules");

        txnPostHistoricalRules.writeTo(Sinks.map(RULESRESULT_MAP, Transaction::getTransactionId, Transaction::getRulesResult));
        txnPostHistoricalRules.writeTo(buildQueueSink());

        log.info(p.toDotString());

        return p;
    }

    private Sink<? super Transaction> buildQueueSink() {
        return SinkBuilder.sinkBuilder("queueSink",
                jet -> jet.hazelcastInstance().<String>getQueue("rules_result_string_queue"))
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
        StreamSource<RestValue> source = SourceBuilder.<QueueContext<RestValue>>stream(TXN_QUEUE_ID, c -> new QueueContext<>(c.hazelcastInstance().getQueue(TXN_QUEUE_ID)))
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
