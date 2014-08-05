package com.metis.monitor.syslog.util.redis;

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Created by Administrator on 14-8-5.
 */
public class RedisClientTest {

    private static RedisClient client;

    @BeforeClass
    public static void setup() {
        client = RedisClient.getInstance("192168-072166", 6379, 1);
    }

    @Test
    public void connectRedis() {
        String redisInfo = client.info();
        System.out.println(redisInfo);
    }

    @Test
    public void setEntry() {
        String result = client.set("foo", "bar");
        System.out.println(result);
    }

    @Test
    public void getEntry() {
        String result = client.get("foo");
        String result2 = client.get("foo2");

        System.out.println(result);
        System.out.println(result2);
    }

    @Test
    public void delEntry() {
        Long result = client.del("foo");
        System.out.println(result);
    }
}
