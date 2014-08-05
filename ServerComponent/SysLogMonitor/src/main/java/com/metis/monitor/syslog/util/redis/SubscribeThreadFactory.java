package com.metis.monitor.syslog.util.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Administrator on 14-8-5.
 */
public class SubscribeThreadFactory implements ThreadFactory {

    private static Logger logger = LoggerFactory.getLogger(SubscribeThreadFactory.class);
    static final AtomicInteger poolCounter = new AtomicInteger(1);
    final AtomicLong threadCounter = new AtomicLong(1);
    final ThreadGroup group;
    final String namingPattern;

    public SubscribeThreadFactory() {
        SecurityManager securityManager = System.getSecurityManager();
        group = (securityManager != null) ? securityManager.getThreadGroup() : Thread.currentThread().getThreadGroup();
        namingPattern = "redis-" + poolCounter.getAndIncrement() + "-subscribe-thread-";
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread worker = new Thread(group, r, namingPattern + threadCounter.getAndIncrement(), 0);
        worker.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                if (logger.isErrorEnabled()) {
                    logger.error(e.getMessage(), e);
                }
            }
        });
        if (worker.isDaemon()) {
            worker.setDaemon(false);
        }
        if (worker.getPriority() != Thread.NORM_PRIORITY) {
            worker.setPriority(Thread.NORM_PRIORITY);
        }
        return worker;
    }
}
