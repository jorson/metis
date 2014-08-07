package com.metis.monitor.syslog.kafka;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * Created by Administrator on 14-4-2.
 */
public class SimplePartitioner implements Partitioner {

    public SimplePartitioner(VerifiableProperties props){

    }

    @Override
    public int partition(Object key, int partitions) {
        int partition = 0;
        String strKey = key.toString();
        int offset = strKey.toString().lastIndexOf(".");
        if(offset > 0){
            partition = Integer.parseInt(strKey.substring(offset+1)) % partitions;
        }
        return partition;
    }
}
