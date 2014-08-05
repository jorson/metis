package com.metis.monitor.syslog.util.redis;


import redis.clients.jedis.Jedis;

/**
 * Created by jorson on 14-8-5.
 */
public interface Action {

    Object execute(final Jedis jedis);
}
