package com.huayu.meits.storm.kafka.trident;

import com.huayu.meits.storm.kafka.KafkaUtils;
import storm.trident.spout.IOpaquePartitionedTridentSpout;
import storm.trident.spout.IPartitionedTridentSpout;

import java.io.IOException;
import java.util.Map;

/**
 * Date: 11/05/2013
 * Time: 19:35
 */
class Coordinator implements IPartitionedTridentSpout.Coordinator<GlobalPartitionInformation>, IOpaquePartitionedTridentSpout.Coordinator<GlobalPartitionInformation> {

    private IBrokerReader reader;
    private TridentKafkaConfig config;

    public Coordinator(Map conf, TridentKafkaConfig tridentKafkaConfig) {
        config = tridentKafkaConfig;
        reader = KafkaUtils.makeBrokerReader(conf, config);
    }

    @Override
    public void close() {
        config.coordinator.close();
    }

    @Override
    public boolean isReady(long txid) {
        return config.coordinator.isReady(txid);
    }

    @Override
    public GlobalPartitionInformation getPartitionsForBatch() {
        return reader.getCurrentBrokers();
    }
}
