package com.huayu.meits.storm.kafka.trident;

import com.huayu.meits.storm.kafka.BrokerHosts;
import com.huayu.meits.storm.kafka.KafkaConfig;


public class TridentKafkaConfig extends KafkaConfig {


    public final IBatchCoordinator coordinator = new DefaultCoordinator();

    public TridentKafkaConfig(BrokerHosts hosts, String topic) {
        super(hosts, topic);
    }

    public TridentKafkaConfig(BrokerHosts hosts, String topic, String clientId) {
        super(hosts, topic, clientId);
    }

}
