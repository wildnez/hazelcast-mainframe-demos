package com.hazelcast.certification.server;

import com.hazelcast.certification.util.MyProperties;
import com.hazelcast.config.*;
import com.hazelcast.spi.merge.PassThroughMergePolicy;


class MapConfigFactory {

    private static final String ACCOUNT_MAP = MyProperties.ACCOUNT_MAP;
    private static final String MERCHANT_MAP = MyProperties.MERCHANT_MAP;
    private static final String RULESRESULT_MAP = MyProperties.RULESRESULT_MAP;

    static void updateWithMapConfig(Config config) {
        MapConfig accountMapConfig = new MapConfig(ACCOUNT_MAP);
        MapStoreConfig accountStoreConfig = new MapStoreConfig();
        accountStoreConfig.setEnabled(true)
                .setClassName("com.hazelcast.certification.domainstore.AccountStore")
                .setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER);
        accountMapConfig.setMapStoreConfig(accountStoreConfig);
        accountMapConfig.setWanReplicationRef(getWanReplicationRefForMaps());
        accountMapConfig.setMerkleTreeConfig(getMerkleTreeConfigForMaps());


        MapConfig merchantMapConfig = new MapConfig(MERCHANT_MAP);
        MapStoreConfig merchantStoreConfig = new MapStoreConfig();
        merchantStoreConfig.setEnabled(true)
                .setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER)
                .setClassName("com.hazelcast.certification.domainstore.MerchantStore");
        merchantMapConfig.setMapStoreConfig(merchantStoreConfig);
        merchantMapConfig.setWanReplicationRef(getWanReplicationRefForMaps());
        merchantMapConfig.setMerkleTreeConfig(getMerkleTreeConfigForMaps());

        MapConfig rulesResultMapConfig = new MapConfig(RULESRESULT_MAP);
        rulesResultMapConfig.setWanReplicationRef(getWanReplicationRefForMaps());
        rulesResultMapConfig.setMerkleTreeConfig(getMerkleTreeConfigForMaps());

        config.addMapConfig(accountMapConfig);
        config.addMapConfig(merchantMapConfig);
        config.addMapConfig(rulesResultMapConfig);

        if(MyProperties.WAN_ENABLED)
            config.addWanReplicationConfig(getWanReplicationConfig());

    }

    private static WanReplicationConfig getWanReplicationConfig() {
        WanReplicationConfig wrConfig = new WanReplicationConfig();
        wrConfig.setName("local");

        WanSyncConfig syncConfig = new WanSyncConfig();
        syncConfig.setConsistencyCheckStrategy(ConsistencyCheckStrategy.MERKLE_TREES);

        WanBatchPublisherConfig wanPublisherConfig = new WanBatchPublisherConfig();
        wanPublisherConfig.setClusterName(MyProperties.WAN_TARGET_NAME)
                .setPublisherId(MyProperties.WAN_TARGET_NAME)
                //.setSyncConfig(syncConfig)
                .setDiscoveryPeriodSeconds(20)
                .setQueueFullBehavior(WanQueueFullBehavior.THROW_EXCEPTION)
                .setQueueCapacity(20000)
                .setBatchSize(10)
                .setBatchMaxDelayMillis(1000)
                .setResponseTimeoutMillis(10000)
                .setSnapshotEnabled(false)
                .setAcknowledgeType(WanAcknowledgeType.ACK_ON_OPERATION_COMPLETE)
                .setTargetEndpoints(MyProperties.WAN_TARGET_URL);
        wrConfig.addBatchReplicationPublisherConfig(wanPublisherConfig);
        return wrConfig;
    }

    private static WanReplicationRef getWanReplicationRefForMaps() {
        WanReplicationRef wanRef = new WanReplicationRef();
        wanRef.setName("local");
        wanRef.setMergePolicyClassName(PassThroughMergePolicy.class.getName());
        wanRef.setRepublishingEnabled(false);

        applyWanFilterForMapStoreEvents(wanRef);
        return wanRef;
    }

    private static MerkleTreeConfig getMerkleTreeConfigForMaps() {
        MerkleTreeConfig merkleTreeConfig = new MerkleTreeConfig();
        merkleTreeConfig.setEnabled(false);
        merkleTreeConfig.setDepth(5);

        return merkleTreeConfig;
    }

    private static void applyWanFilterForMapStoreEvents(WanReplicationRef wanRef) {
        wanRef.addFilter("com.hazelcast.certification.util.WanFilterForMapStores");
    }

}
