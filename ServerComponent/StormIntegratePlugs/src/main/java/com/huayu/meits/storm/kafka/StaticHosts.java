package com.huayu.meits.storm.kafka;

import com.huayu.meits.storm.kafka.trident.GlobalPartitionInformation;

/**
 * Date: 11/05/2013
 * Time: 14:43
 */
public class StaticHosts implements BrokerHosts {


    private GlobalPartitionInformation partitionInformation;

    public StaticHosts(GlobalPartitionInformation partitionInformation) {
        this.partitionInformation = partitionInformation;
    }

    public GlobalPartitionInformation getPartitionInformation() {
        return partitionInformation;
    }
}
