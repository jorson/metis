package com.huayu.meits.storm.kafka.trident;

public interface IBrokerReader {

    GlobalPartitionInformation getCurrentBrokers();

    void close();
}
