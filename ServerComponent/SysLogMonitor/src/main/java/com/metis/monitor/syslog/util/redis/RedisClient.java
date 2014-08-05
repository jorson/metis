package com.metis.monitor.syslog.util.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.util.Slowlog;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
    private int readStatus = 0;

    private int delayRetry = 10;
    private AliveEventListener aliveEventListener;
    private ExecutorService executor;

    /*Instance Method*/
    public static RedisClient getInstance() {
        synchronized (RedisClient.class) {
            if(instance == null) {
                synchronized (RedisClient.class) {
                    instance = new RedisClient();
                }
            }
        }
        return instance;
    }

    public static RedisClient getInstance(String host, int port) {
        synchronized (RedisClient.class) {
            if(instance == null) {
                synchronized (RedisClient.class) {
                    instance = new RedisClient(host, port);
                }
            }
        }
        return instance;
    }

    public static RedisClient getInstance(String host, int port, String password, Integer database) {
        synchronized (RedisClient.class) {
            if(instance == null) {
                synchronized (RedisClient.class) {
                    instance = new RedisClient(host, port, password, database);
                }
            }
        }
        return instance;
    }

    public static RedisClient getInstance(String host, int port, Integer database) {
        synchronized (RedisClient.class) {
            if(instance == null) {
                synchronized (RedisClient.class) {
                    instance = new RedisClient(host, port, null, database);
                }
            }
        }
        return instance;
    }

    private RedisClient() {
        this("127.0.0.1", 6379);
    }

    private RedisClient(String host, int port) {
        this(host, port, null, 0);
    }

    private RedisClient(String host, int port, String password, Integer database) {
        this.masterHost = host;
        this.masterPort = port;
        this.password = password != null && password.isEmpty() ? null : password;
        this.database = database == null ? 0 : database;

        //开启连接
        this.connect();
        this.executor = Executors.newFixedThreadPool(2, new SubscribeThreadFactory());
        this.checkAlive();
    }

    private void connect() {
        if(writeStatus != 1) {
            initMaster(getDefaultConfig());
            initSlaves(getDefaultConfig());
        }
        if(readStatus != 1) {
            initSlaves(getDefaultConfig());
        }

        if(this.aliveEventListener != null) {
            this.aliveEventListener.onEvents(masterOnline(), slaveOnline());
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

                            if(host.contains("=")) {
                                host = host.split("=")[1];
                            }
                            if(port.contains("=")) {
                                port = port.split("=")[1];
                            }


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
                    readStatus = !slavePools.isEmpty() ? 1 : -1;
                    switch (readStatus) {
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
                    return readStatus;
                }
            });
        }
    }

    private boolean slaveOnline() {
        return this.readStatus == 1;
    }

    private boolean masterOnline() {
        return this.writeStatus == 1;
    }

    private void sleep(int second) {
        try {
            TimeUnit.SECONDS.sleep(second);
        } catch (InterruptedException ig) {

        }
    }

    private void checkAlive() {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try{
                        connect();
                        sleep(delayRetry);
                    } catch (JedisConnectionException e) {
                        if(logger.isErrorEnabled()) {
                            logger.error("redis db connection reset ! please check it , {} second after reconnect !", delayRetry);
                        }
                        sleep(delayRetry);
                    }
                }
            }
        });
    }

    private JedisPoolConfig getDefaultConfig() {
        // 连接配置
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxActive(100);
        config.setMaxIdle(20);
        config.setMaxWait(10000);
        // 设定在借出对象时是否进行有效性检查
        config.setTestOnBorrow(true);
        return config;
    }

    public String set(final String key, final String value) {
        Object actual = master(new Action() {
            @Override
            public Object execute(Jedis jedis) {
                return jedis.set(key, value);
            }
        });
        return actual != null ? actual.toString() : null;
    }

    public Long hset(final String key, final String field, final String value) {
        Object actual = master(new Action() {
            @Override
            public Object execute(Jedis jedis) {
                return jedis.hset(key, field, value);
            }
        });
        return actual != null ? Long.valueOf(actual.toString()) : null;
    }

    public boolean exists(final String key) {
        Object actual = slave(new Action() {
            @Override
            public Object execute(Jedis jedis) {
                return jedis.exists(key);
            }
        });
        return actual != null ? (Boolean)actual : false;
    }

    public String get(final String key) {
        Object actual = slave(new Action() {
            @Override
            public Object execute(Jedis jedis) {
                return jedis.get(key);
            }
        });
        return actual != null ? actual.toString() : null;
    }

    public boolean exists(final String key, final String field) {
        Object actual = slave(new Action() {
            @Override
            public Object execute(Jedis jedis) {
                return jedis.hexists(key, field);
            }
        });
        return actual != null ? (Boolean)actual : false;
    }

    public boolean hexists(final String key, final String field) {
        Object actual = slave(new Action() {
            @Override
            public Object execute(Jedis jedis) {
                return jedis.hexists(key, field);
            }
        });
        return actual != null ? (Boolean)actual : false;
    }

    public String hget(final String key, final String field) {
        Object actual = slave(new Action() {
            @Override
            public Object execute(Jedis jedis) {
                return jedis.hget(key, field);
            }
        });
        return actual != null ? actual.toString() : null;
    }

    public Long hlen(final String key) {
        Object actual = slave(new Action() {
            @Override
            public Object execute(Jedis jedis) {
                return jedis.hlen(key);
            }
        });
        return actual != null ? Long.valueOf(actual.toString()) : null;
    }

    public Set<String> hkeys(final String key) {
        Object actual = slave(new Action() {
            @Override
            public Object execute(Jedis jedis) {
                return jedis.hkeys(key);
            }
        });
        return actual != null ? (Set<String>)actual : null;
    }

    public Map<String, String> hgetAll(final String key) {
        Object actual = slave(new Action() {
            @Override
            public Object execute(Jedis jedis) {
                return jedis.hgetAll(key);
            }
        });
        return actual != null ? (Map<String, String>)actual : null;
    }

    public Set<String> keys(final String pattern) {
        Object actual = master(new Action() {
            @Override
            public Object execute(Jedis jedis) {
                return jedis.keys(pattern);
            }
        });
        return actual != null ? (Set<String>)actual : null;
    }

    public Long publish(final String channel, final String command, final String message) {
        long actual = 0;
        if(command != null && message != null) {
            actual = publish(channel, String.format("%s %s", command, message));
        }
        return actual;
    }

    public Long publish(final String channel, final String message) {
        Object actual = master(new Action() {
            @Override
            public Object execute(final Jedis jedis) {
                return jedis.publish(channel, message);
            }
        });
        return actual != null ? (Long) actual : 0;
    }

    public void subscribe(final JedisPubSub handler, final String... channels) {
        // 订阅会阻塞进程，因此开一个新线程来执行
        executor.execute(new Runnable() {
            @Override
            public void run() {
                int delayRetry = 10;
                JedisShardInfo info = new JedisShardInfo(masterHost, masterPort, defaultTimeout);
                if (password != null) {
                    info.setPassword(password);
                }
                // jedis.select(database); 每个Redis Server只有一套订阅，与数据库无关，不需要关注select 那个database
                while (true) {
                    try {
                        Jedis jedis = new Jedis(info);
                        jedis.subscribe(handler, channels);
                    } catch (JedisConnectionException e) {
                        writeStatus = -1;
                        if (logger.isErrorEnabled()) {
                            logger.error("redis db connection reset ! please check it , {} second after reconnect !", delayRetry);
                        }
                        sleep(delayRetry);
                    }
                }
            }
        });
    }

    public boolean pingMaster() {
        Object actual = master(new Action() {
            @Override
            public Object execute(final Jedis jedis) {
                String pong = null;
                try {
                    pong = jedis.ping();
                } catch (Exception ignored) {
                }
                return pong;
            }
        });
        return true;
    }

    public boolean pingSlave() {
        Object actual = slave(new Action() {
            @Override
            public Object execute(final Jedis jedis) {
                String pong = null;
                try {
                    pong = jedis.ping();
                } catch (Exception ignored) {
                }
                return pong;
            }
        });
        return true;
    }

    public Long del(final String... keys) {
        Object actual = master(new Action() {
            @Override
            public Object execute(Jedis jedis) {
                return jedis.del(keys);
            }
        });
        return actual != null ? (Long)actual : null;
    }

    public Long size() {
        Object actual = master(new Action() {
            @Override
            public Object execute(Jedis jedis) {
                return null;
            }
        });
        return actual != null ? (Long)actual : null;
    }

    public String bgrewriteaof() {
        Object actual = master(new Action() {
            @Override
            public Object execute(final Jedis jedis) {
                return jedis.bgrewriteaof();
            }
        });
        return actual != null ? (String) actual : null;
    }

    public String bgsave() {
        Object actual = master(new Action() {
            @Override
            public Object execute(final Jedis jedis) {
                return jedis.bgsave();
            }
        });
        return actual != null ? (String) actual : null;
    }

    public String save() {
        Object actual = master(new Action() {
            @Override
            public Object execute(final Jedis jedis) {
                return jedis.save();
            }
        });
        return actual != null ? (String) actual : null;
    }

    public Long lastSave() {
        Object actual = master(new Action() {
            @Override
            public Object execute(final Jedis jedis) {
                return jedis.lastsave();
            }
        });
        return actual != null ? (Long) actual : null;
    }

    public String slaveof(final String host, final Integer port) {
        Object actual = slave(new Action() {
            @Override
            public Object execute(final Jedis jedis) {
                return jedis.slaveof(host, port);
            }
        });
        return actual != null ? (String) actual : null;
    }

    public String slaveofNoOne() {
        Object actual = slave(new Action() {
            @Override
            public Object execute(final Jedis jedis) {
                return jedis.slaveofNoOne();
            }
        });
        return actual != null ? (String) actual : null;
    }

    public String flushAll() {
        Object actual = master(new Action() {
            @Override
            public Object execute(final Jedis jedis) {
                return jedis.flushAll();
            }
        });
        return actual != null ? (String) actual : null;
    }

    public String flushDB() {
        Object actual = master(new Action() {
            @Override
            public Object execute(final Jedis jedis) {
                return jedis.flushDB();
            }
        });
        return actual != null ? (String) actual : null;
    }

    public String shutdown() {
        Object actual = master(new Action() {
            @Override
            public Object execute(final Jedis jedis) {
                return jedis.shutdown();
            }
        });
        return actual != null ? (String) actual : null;
    }

    public List<Slowlog> slowlogGet() {
        Object actual = slave(new Action() {
            @Override
            public Object execute(final Jedis jedis) {
                return jedis.slowlogGet();
            }
        });
        return actual != null ? (List<Slowlog>) actual : null;
    }

    public List<Slowlog> slowlogGet(final Long entries) {
        Object actual = slave(new Action() {
            @Override
            public Object execute(final Jedis jedis) {
                return jedis.slowlogGet(entries);
            }
        });
        return actual != null ? (List<Slowlog>) actual : null;
    }

    public Long slowlogLen() {
        Object actual = slave(new Action() {
            @Override
            public Object execute(final Jedis jedis) {
                return jedis.slowlogLen();
            }
        });
        return actual != null ? (Long) actual : null;
    }

    public byte[] slowlogReset() {
        Object actual = master(new Action() {
            @Override
            public Object execute(final Jedis jedis) {
                return jedis.slowlogReset();
            }
        });
        return actual != null ? (byte[]) actual : null;
    }

    public String info() {
        Object actual = master(new Action() {
            @Override
            public Object execute(final Jedis jedis) {
                return jedis.info();
            }
        });
        return actual != null ? (String) actual : null;
    }

    public List<String> configGet(final String pattern) {
        Object actual = slave(new Action() {
            @Override
            public Object execute(final Jedis jedis) {
                return jedis.configGet(pattern);
            }
        });
        return actual != null ? (List<String>) actual : null;
    }

    public String configSet(final String parameter, final String value) {
        Object actual = master(new Action() {
            @Override
            public Object execute(final Jedis jedis) {
                return jedis.configSet(parameter, value);
            }
        });
        return actual != null ? (String) actual : null;
    }

    public String configResetStart() {
        Object actual = master(new Action() {
            @Override
            public Object execute(final Jedis jedis) {
                return jedis.configResetStat();
            }
        });
        return actual != null ? (String) actual : null;
    }

    public Object slave(Action action) {
        Object actual = null;

        if(slaveOnline()) {
            JedisPool pool = null;
            Jedis jedis = null;
            try {
                pool = this.getSlavePool();
                if(pool != null) {
                    jedis = pool.getResource();
                    actual = action.execute(jedis);
                } else {
                    this.readStatus = -1;
                }
            } catch (JedisConnectionException e) {
                int osize = slavePools.size();
                initSlaves(getDefaultConfig());
                int nsize = slavePools.size();
                this.readStatus = slavePools.isEmpty() ? -1 : 1;
                if(osize != nsize && logger.isErrorEnabled()) {
                    logger.error("slave instance size changed! {} to {}", osize, nsize);
                }
            } finally {
                if(pool != null) {
                    pool.returnResource(jedis);
                }
            }
        }
        return actual;
    }

    public Object master(Action action) {
        Object actual = null;
        if(masterOnline()) {
            JedisPool pool = null;
            Jedis jedis = null;

            try {
                pool = this.getMasterPool();
                jedis = pool.getResource();
                actual = action.execute(jedis);
            } catch (JedisConnectionException e) {
                this.writeStatus = -1;
                if(logger.isErrorEnabled()) {
                    logger.error("redis db connection reset ! please checkAlive it !");
                }
            } finally {
                if(pool != null) {
                    pool.returnResource(jedis);
                }
            }
        }
        return actual;
    }

    public JedisPool getMasterPool() {
        return masterPool;
    }

    public JedisPool getSlavePool() {
        JedisPool pool = slavePools.get(poolIndex);
        poolIndex += 1;
        if(poolIndex > slavePools.size()) {
            poolIndex = 1;
        }
        return pool;
    }

    public interface AliveEventListener {
        void onEvents(boolean masterAlive, boolean slaveAlive);
    }

}
