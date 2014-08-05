package com.metis.monitor.syslog.util.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * Created by jorson on 14-8-5.
 */
public class RedisClient {

    private static Logger logger = LoggerFactory.getLogger(RedisClient.class);

    private static volatile RedisClient instance;

    private int defaultTimeout = 5000;

    private JedisPool masterPool;
    private String masterHost = "127.0.0.1";
    private int masterPort = 6379;

    private Map<Integer, JedisPool> slavePools;

    private static int poolIndex = 1;

    private String password;
    private int database;

    private int writeStatus = 0;
    private int readStatis = 0;

    private int delayRetry = 10;
    private AliveEventListener aliveEventListener;
    private ExecutorService executor;

    private RedisClient() {

    }

    private RedisClient(String host, int port) {

    }

    private RedisClient(String host, int port, String password, Integer database) {
        this.masterHost = host;
        this.masterPort = port;
        this.password = password != null && password.isEmpty() ? null : password;
        this.database = database == null ? 0 : database;

        //开启连接

    }

    private void connect() {
        if(writeStatus != 1) {

        }
    }

    private void initMaster(JedisPoolConfig config) {
        try {
            masterPool = new JedisPool(config, this.masterHost, this.masterPort, this.defaultTimeout,this.password, this.database);
            Jedis jedis = masterPool.getResource();
            jedis.ping();
            masterPool.returnResource(jedis);
            writeStatus = 1;

            if(logger.isInfoEnabled()) {
                logger.info("redis master connected");
            }
        } catch (JedisConnectionException e) {
            writeStatus = -1;
            if(logger.isErrorEnabled()) {
                logger.error("redis master connection reset ! please check it , {} second after reconnect !", delayRetry);
            }
        }
    }

    private void initSlaves(final JedisPoolConfig config) {
        if(masterPool != null) {
            slavePools = new HashMap<Integer, JedisPool>();
            master(new Action(){
                @Override
                public Object execute(Jedis jedis) {
                    String info = jedis.info();
                    String[] lines = info.split("\r\n");
                    for(String line : lines) {
                        if(line.contains("slave") && line.contains("online")) {
                            String[] online = line.split(":")[1].split(",");
                            String host = online[0];
                            String port = online[1];

                            if("127.0.0.1".equalsIgnoreCase(host)){
                                host = masterHost;
                            }
                            jedis.hset("redis.slave", host, port);
                            JedisPool pool = null;
                            Jedis slave = null;

                            try{
                                pool = new JedisPool(config, host, Integer.valueOf(port), defaultTimeout, password, database);
                                slave = pool.getResource();
                                slavePools.put(slavePools.size() + 1, pool);
                            } catch (JedisConnectionException e) {
                                if(logger.isErrorEnabled()) {
                                    logger.error("slave: {}", e.getMessage());
                                }
                            } finally {
                                if(pool != null && slave != null) {
                                    pool.returnResource(slave);
                                }
                            }
                        }
                    }
                    readStatis = !slavePools.isEmpty() ? 1 : -1;
                    switch (readStatis) {
                        case -1:
                            if(logger.isErrorEnabled()) {
                                logger.error("redis slave connection reset ! please check it , {} second after reconnect !", delayRetry);
                            }
                            break;
                        case 1:
                            if (logger.isInfoEnabled()) {
                                logger.info("redis slave connected!");
                            }
                            break;
                    }
                    return readStatis;
                }
            });
        }
    }

    public Long del(final String... keys) {
        Object actual = new Action() {
            @Override
            public Object execute(Jedis jedis) {
                return jedis.del(keys);
            }
        };
        return actual != null ? (Long)actual : null;
    }

    public Long size() {
        Object actual = new Action() {
            @Override
            public Object execute(Jedis jedis) {
                return null;
            }
        };
    }

    public interface AliveEventListener {
        void onEvents(boolean masterAlive, boolean slaveAlive);
    }

}
