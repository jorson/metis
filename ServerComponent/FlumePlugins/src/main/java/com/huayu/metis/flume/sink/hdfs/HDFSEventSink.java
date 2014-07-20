/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huayu.metis.flume.sink.hdfs;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import com.huayu.metis.flume.utility.KafkaFlumeConstans;
import org.apache.flume.Channel;
import org.apache.flume.Clock;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.SystemClock;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.formatter.output.BucketPath;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class HDFSEventSink extends AbstractSink implements Configurable {
    public interface WriterCallback {
        public void run(String filePath);
    }

    private final Charset utf8Code = Charset.forName(KafkaFlumeConstans.DEFAULT_ENCODING);

    private static final Logger LOG = LoggerFactory.getLogger(HDFSEventSink.class);

    private static String DIRECTORY_DELIMITER = System.getProperty("file.separator");
    //默认不要按照时间来轮转文件
    private static final long defaultRollInterval = 0;
    //默认不按照文件的大小进行文件轮转
    private static final long defaultRollSize = 0;
    //默认按照写入的事件数量进行文件轮转
    private static final long defaultRollCount = 5000;
    private static final String defaultFileName = "LogData";
    private static final String defaultSuffix = "log";
    private static final String defaultInUseSuffix = ".tmp";
    //每批次写入的事件数量
    private static final long defaultBatchSize = 100;
    private static final String defaultFileType = HDFSWriterFactory.SequenceFileType;
    private static final int defaultMaxOpenFiles = 5000;

    /**
     * Default length of time we wait for blocking BucketWriter calls
     * before timing out the operation. Intended to prevent server hangs.
     */
    private static final long defaultCallTimeout = 10000;
    /**
     * Default number of threads available for tasks
     * such as append/open/close/flush with hdfs.
     * These tasks are done in a separate thread in
     * the case that they take too long. In which
     * case we create a new file and move on.
     */
    private static final int defaultThreadPoolSize = 10;
    private static final int defaultRollTimerPoolSize = 1;

    /**
     * Singleton credential manager that manages static credentials for the
     * entire JVM
     */
    private static final AtomicReference<KerberosUser> staticLogin
            = new AtomicReference<KerberosUser>();

    private final HDFSWriterFactory writerFactory;
    private WriterLinkedHashMap sfWriters;

    private long rollInterval;
    private long rollSize;
    private long rollCount;
    private long batchSize;
    private int threadsPoolSize;
    private int rollTimerPoolSize;
    private int maxRotations;
    private String fileType;
    private String filePath;
    private String fileName;
    private String suffix;
    private String inUseSuffix;
    private int maxOpenFiles;
    private ExecutorService callTimeoutPool;
    private ScheduledExecutorService timedRollerPool;

    private String kerbConfPrincipal;
    private String kerbKeytab;
    private String proxyUserName;
    private UserGroupInformation proxyTicket;

    private boolean needRounding = false;
    private int roundUnit = Calendar.SECOND;
    private int roundValue = 1;

    private long callTimeout;
    private Context context;
    private SinkCounter sinkCounter;

    private volatile int idleTimeout;
    private Clock clock;
    private final Object sfWritersLock = new Object();

    /*
     * Extended Java LinkedHashMap for open file handle LRU queue.
     * We want to clear the oldest file handle if there are too many open ones.
     */
    private static class WriterLinkedHashMap
            extends LinkedHashMap<String, BucketWriter> {

        private final int maxOpenFiles;

        public WriterLinkedHashMap(int maxOpenFiles) {
            super(16, 0.75f, true); // stock initial capacity/load, access ordering
            this.maxOpenFiles = maxOpenFiles;
        }

        @Override
        protected boolean removeEldestEntry(Entry<String, BucketWriter> eldest) {
            if (size() > maxOpenFiles) {
                // If we have more that max open files, then close the last one and
                // return true
                try {
                    eldest.getValue().close();
                } catch (IOException e) {
                    LOG.warn(eldest.getKey().toString(), e);
                } catch (InterruptedException e) {
                    LOG.warn(eldest.getKey().toString(), e);
                    Thread.currentThread().interrupt();
                }
                return true;
            } else {
                return false;
            }
        }
    }

    public HDFSEventSink() {
        this(new HDFSWriterFactory());
    }

    public HDFSEventSink(HDFSWriterFactory writerFactory) {
        this.writerFactory = writerFactory;
    }

    @VisibleForTesting
    Map<String, BucketWriter> getSfWriters() {
        return sfWriters;
    }

    // read configuration and setup thresholds
    @Override
    public void configure(Context context) {
        this.context = context;

        this.filePath = Preconditions.checkNotNull(context.getString("hdfs.path"), "hdfs.path is required");
        this.fileName = context.getString("hdfs.filePrefix", defaultFileName);
        this.suffix = defaultSuffix;
        inUseSuffix = defaultInUseSuffix;
        rollInterval = context.getLong("hdfs.rollInterval", defaultRollInterval);
        rollSize = context.getLong("hdfs.rollSize", defaultRollSize);
        rollCount = context.getLong("hdfs.rollCount", defaultRollCount);
        maxRotations = context.getInteger("hdfs.maxRotations", 0);  //0表示不限制
        batchSize = context.getLong("hdfs.batchSize", defaultBatchSize);
        idleTimeout = context.getInteger("hdfs.idleTimeout", 0);
        fileType = context.getString("hdfs.fileType", defaultFileType);
        maxOpenFiles = context.getInteger("hdfs.maxOpenFiles", defaultMaxOpenFiles);
        callTimeout = context.getLong("hdfs.callTimeout", defaultCallTimeout);
        threadsPoolSize = context.getInteger("hdfs.threadsPoolSize", defaultThreadPoolSize);
        rollTimerPoolSize = context.getInteger("hdfs.rollTimerPoolSize", defaultRollTimerPoolSize);
        kerbConfPrincipal = context.getString("hdfs.kerberosPrincipal", "");
        kerbKeytab = context.getString("hdfs.kerberosKeytab", "");
        proxyUserName = context.getString("hdfs.proxyUser", "");

        Preconditions.checkArgument(batchSize > 0, "batchSize must be greater than 0");

        if (!authenticate()) {
            LOG.error("Failed to authenticate!");
        }
        needRounding = context.getBoolean("hdfs.round", false);

        if (needRounding) {
            String unit = context.getString("hdfs.roundUnit", "second");
            if (unit.equalsIgnoreCase("hour")) {
                this.roundUnit = Calendar.HOUR_OF_DAY;
            } else if (unit.equalsIgnoreCase("minute")) {
                this.roundUnit = Calendar.MINUTE;
            } else if (unit.equalsIgnoreCase("second")) {
                this.roundUnit = Calendar.SECOND;
            } else {
                LOG.warn("Rounding unit is not valid, please set one of minute, hour, or second. Rounding will be disabled");
                needRounding = false;
            }
            this.roundValue = context.getInteger("hdfs.roundValue", 1);
            if (roundUnit == Calendar.SECOND || roundUnit == Calendar.MINUTE) {
                Preconditions.checkArgument(roundValue > 0 && roundValue <= 60, "Round value must be > 0 and <= 60");
            } else if (roundUnit == Calendar.HOUR_OF_DAY) {
                Preconditions.checkArgument(roundValue > 0 && roundValue <= 24, "Round value must be > 0 and <= 24");
            }
        }

        clock = new SystemClock();
        if (sinkCounter == null) {
            sinkCounter = new SinkCounter(getName());
        }
    }

    /**
     * 将事件从Channel中拉取出来, 并写入到HDFS中,
     * 每次获取事件的数量,有Config中的BatchSize确定
     * 确认待写入的文件是打开的, 序列化数据并写入
     * </br>
     * 这是一个非线程安全的方法
     */
    public Status process() throws EventDeliveryException {
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        List<BucketWriter> writers = Lists.newArrayList();
        boolean readEventNull = false;

        transaction.begin();
        try {
            int txnEventCount = 0;
            for (txnEventCount = 0; txnEventCount < batchSize; txnEventCount++) {
                Event event = channel.take();
                if (event == null) {
                    readEventNull = true;
                    break;
                }

                //如果获取到事件, 开始写入
                //从事件的Header中获取Topic和AppId,并从配置的文件路径中替换掉
                String formatFilePath = transFilePath(event);
                //将文件路径和文件名中日期换掉
                String realPath = BucketPath.escapeString(formatFilePath, event.getHeaders(),
                        null, needRounding, roundUnit, roundValue, true);
                String realName = BucketPath.escapeString(this.fileName, event.getHeaders(),
                        null, needRounding, roundUnit, roundValue, true);

                //构建查找路径
                //Topic + "/" + Path + "/" + Name
                String lookupPath = realPath + DIRECTORY_DELIMITER + realName;
                BucketWriter bucketWriter;
                HDFSWriter hdfsWriter = null;
                // Callback to remove the reference to the bucket writer from the
                // sfWriters map so that all buffers used by the HDFS file
                // handles are garbage collected.
                WriterCallback closeCallback = new WriterCallback() {
                    @Override
                    public void run(String bucketPath) {
                        LOG.info("Writer callback called.");
                        synchronized (sfWritersLock) {
                            sfWriters.remove(bucketPath);
                        }
                    }
                };
                synchronized (sfWritersLock) {
                    bucketWriter = sfWriters.get(lookupPath);
                    // we haven't seen this file yet, so open it and cache the handle
                    if (bucketWriter == null) {
                        hdfsWriter = writerFactory.getWriter(fileType);
                        bucketWriter = initializeBucketWriter(realPath, realName, lookupPath, hdfsWriter, closeCallback);
                        sfWriters.put(lookupPath, bucketWriter);
                    }
                }

                // track the buckets getting written in this transaction
                if (!writers.contains(bucketWriter)) {
                    LOG.info("[hy] Add a BucketWriter Key:" + lookupPath);
                    writers.add(bucketWriter);
                }

                // Write the data to HDFS
                try {
                    String body = new String(event.getBody(), utf8Code);
                    LOG.info("[hy] Append To File:" + lookupPath + ", Event:" + body);
                    bucketWriter.append(event);
                } catch (BucketClosedException ex) {
                    LOG.info("[hy]Bucket was closed while trying to append,reinitializing bucket and writing event.");
                    hdfsWriter = writerFactory.getWriter(fileType);
                    bucketWriter = initializeBucketWriter(realPath, realName, lookupPath, hdfsWriter, closeCallback);
                    synchronized (sfWritersLock) {
                        sfWriters.put(lookupPath, bucketWriter);
                    }
                    bucketWriter.append(event);
                }
            }

            if (txnEventCount == 0) {
                sinkCounter.incrementBatchEmptyCount();
            } else if (txnEventCount == batchSize) {
                sinkCounter.incrementBatchCompleteCount();
            } else {
                sinkCounter.incrementBatchUnderflowCount();
            }

            //在提交事务前将所有桶的数据Flush
            for (BucketWriter bucketWriter : writers) {
                bucketWriter.flush();
                if(readEventNull){
                    bucketWriter.close(true);
                }
            }

            transaction.commit();

            if (txnEventCount < 1) {
                return Status.BACKOFF;
            } else {
                sinkCounter.addToEventDrainSuccessCount(txnEventCount);
                return Status.READY;
            }
        } catch (IOException eIO) {
            transaction.rollback();
            LOG.warn("HDFS IO error", eIO);
            return Status.BACKOFF;
        } catch (Throwable th) {
            transaction.rollback();
            LOG.error("process failed", th);
            if (th instanceof Error) {
                throw (Error) th;
            } else {
                throw new EventDeliveryException(th);
            }
        } finally {
            transaction.close();
        }
    }

    private BucketWriter initializeBucketWriter(String realPath, String realName, String lookupPath,
                                                HDFSWriter hdfsWriter, WriterCallback closeCallback) {
        return new BucketWriter(rollInterval, rollSize, rollCount,
                batchSize, context, realPath, realName, inUseSuffix,
                suffix, hdfsWriter, timedRollerPool,
                proxyTicket, sinkCounter, idleTimeout, closeCallback,
                lookupPath, callTimeout, callTimeoutPool, maxRotations);
    }

    @Override
    public void stop() {
        // do not constrain close() calls with a timeout
        for (Entry<String, BucketWriter> entry : sfWriters.entrySet()) {
            LOG.info("Closing {}", entry.getKey());

            try {
                entry.getValue().close();
            } catch (Exception ex) {
                LOG.warn("Exception while closing " + entry.getKey() + ".Exception follows.", ex);
                if (ex instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        // shut down all our thread pools
        ExecutorService toShutdown[] = {callTimeoutPool, timedRollerPool};
        for (ExecutorService execService : toShutdown) {
            execService.shutdown();
            try {
                while (execService.isTerminated() == false) {
                    execService.awaitTermination(
                            Math.max(defaultCallTimeout, callTimeout), TimeUnit.MILLISECONDS);
                }
            } catch (InterruptedException ex) {
                LOG.warn("shutdown interrupted on " + execService, ex);
            }
        }

        callTimeoutPool = null;
        timedRollerPool = null;

        sfWriters.clear();
        sfWriters = null;
        sinkCounter.stop();
        super.stop();
    }

    @Override
    public void start() {
        String timeoutName = "hdfs-" + getName() + "-call-runner-%d";
        callTimeoutPool = Executors.newFixedThreadPool(threadsPoolSize, new ThreadFactoryBuilder().setNameFormat(timeoutName).build());

        String rollerName = "hdfs-" + getName() + "-roll-timer-%d";
        timedRollerPool = Executors.newScheduledThreadPool(rollTimerPoolSize, new ThreadFactoryBuilder().setNameFormat(rollerName).build());

        this.sfWriters = new WriterLinkedHashMap(maxOpenFiles);
        sinkCounter.start();
        super.start();
    }

    private boolean authenticate() {

        // logic for kerberos login
        boolean useSecurity = UserGroupInformation.isSecurityEnabled();

        LOG.info("Hadoop Security enabled: " + useSecurity);

        if (useSecurity) {

            // sanity checking
            if (kerbConfPrincipal.isEmpty()) {
                LOG.error("Hadoop running in secure mode, but Flume config doesn't "
                        + "specify a principal to use for Kerberos auth.");
                return false;
            }
            if (kerbKeytab.isEmpty()) {
                LOG.error("Hadoop running in secure mode, but Flume config doesn't "
                        + "specify a keytab to use for Kerberos auth.");
                return false;
            } else {
                //If keytab is specified, user should want it take effect.
                //HDFSEventSink will halt when keytab file is non-exist or unreadable
                File kfile = new File(kerbKeytab);
                if (!(kfile.isFile() && kfile.canRead())) {
                    throw new IllegalArgumentException("The keyTab file: "
                            + kerbKeytab + " is nonexistent or can't read. "
                            + "Please specify a readable keytab file for Kerberos auth.");
                }
            }

            String principal;
            try {
                // resolves _HOST pattern using standard Hadoop search/replace
                // via DNS lookup when 2nd argument is empty
                principal = SecurityUtil.getServerPrincipal(kerbConfPrincipal, "");
            } catch (IOException e) {
                LOG.error("Host lookup error resolving kerberos principal ("
                        + kerbConfPrincipal + "). Exception follows.", e);
                return false;
            }

            Preconditions.checkNotNull(principal, "Principal must not be null");
            KerberosUser prevUser = staticLogin.get();
            KerberosUser newUser = new KerberosUser(principal, kerbKeytab);

            // be cruel and unusual when user tries to login as multiple principals
            // this isn't really valid with a reconfigure but this should be rare
            // enough to warrant a restart of the agent JVM
            // TODO: find a way to interrogate the entire current config state,
            // since we don't have to be unnecessarily protective if they switch all
            // HDFS sinks to use a different principal all at once.
            Preconditions.checkState(prevUser == null || prevUser.equals(newUser),
                    "Cannot use multiple kerberos principals in the same agent. " +
                            " Must restart agent to use new principal or keytab. " +
                            "Previous = %s, New = %s", prevUser, newUser);

            // attempt to use cached credential if the user is the same
            // this is polite and should avoid flooding the KDC with auth requests
            UserGroupInformation curUser = null;
            if (prevUser != null && prevUser.equals(newUser)) {
                try {
                    curUser = UserGroupInformation.getLoginUser();
                } catch (IOException e) {
                    LOG.warn("User unexpectedly had no active login. Continuing with " +
                            "authentication", e);
                }
            }

            if (curUser == null || !curUser.getUserName().equals(principal)) {
                try {
                    // static login
                    kerberosLogin(this, principal, kerbKeytab);
                } catch (IOException e) {
                    LOG.error("Authentication or file read error while attempting to "
                            + "login as kerberos principal (" + principal + ") using "
                            + "keytab (" + kerbKeytab + "). Exception follows.", e);
                    return false;
                }
            } else {
                LOG.debug("{}: Using existing principal login: {}", this, curUser);
            }

            // we supposedly got through this unscathed... so store the static user
            staticLogin.set(newUser);
        }

        // hadoop impersonation works with or without kerberos security
        proxyTicket = null;
        if (!proxyUserName.isEmpty()) {
            try {
                proxyTicket = UserGroupInformation.createProxyUser(
                        proxyUserName, UserGroupInformation.getLoginUser());
            } catch (IOException e) {
                LOG.error("Unable to login as proxy user. Exception follows.", e);
                return false;
            }
        }

        UserGroupInformation ugi = null;
        if (proxyTicket != null) {
            ugi = proxyTicket;
        } else if (useSecurity) {
            try {
                ugi = UserGroupInformation.getLoginUser();
            } catch (IOException e) {
                LOG.error("Unexpected error: Unable to get authenticated user after " +
                        "apparent successful login! Exception follows.", e);
                return false;
            }
        }

        if (ugi != null) {
            // dump login information
            AuthenticationMethod authMethod = ugi.getAuthenticationMethod();
            LOG.info("Auth method: {}", authMethod);
            LOG.info(" User name: {}", ugi.getUserName());
            LOG.info(" Using keytab: {}", ugi.isFromKeytab());
            if (authMethod == AuthenticationMethod.PROXY) {
                UserGroupInformation superUser;
                try {
                    superUser = UserGroupInformation.getLoginUser();
                    LOG.info(" Superuser auth: {}", superUser.getAuthenticationMethod());
                    LOG.info(" Superuser name: {}", superUser.getUserName());
                    LOG.info(" Superuser using keytab: {}", superUser.isFromKeytab());
                } catch (IOException e) {
                    LOG.error("Unexpected error: unknown superuser impersonating proxy.",
                            e);
                    return false;
                }
            }

            LOG.info("Logged in as user {}", ugi.getUserName());

            return true;
        }

        return true;
    }

    /**
     * Static synchronized method for static Kerberos login. <br/>
     * Static synchronized due to a thundering herd problem when multiple Sinks
     * attempt to log in using the same principal at the same time with the
     * intention of impersonating different users (or even the same user).
     * If this is not controlled, MIT Kerberos v5 believes it is seeing a replay
     * attach and it returns:
     * <blockquote>Request is a replay (34) - PROCESS_TGS</blockquote>
     * In addition, since the underlying Hadoop APIs we are using for
     * impersonation are static, we define this method as static as well.
     *
     * @param principal Fully-qualified principal to use for authentication.
     * @param keytab    Location of keytab file containing credentials for principal.
     * @return Logged-in user
     * @throws java.io.IOException if login fails.
     */
    private static synchronized UserGroupInformation kerberosLogin(
            HDFSEventSink sink, String principal, String keytab) throws IOException {

        // if we are the 2nd user thru the lock, the login should already be
        // available statically if login was successful
        UserGroupInformation curUser = null;
        try {
            curUser = UserGroupInformation.getLoginUser();
        } catch (IOException e) {
            // not a big deal but this shouldn't typically happen because it will
            // generally fall back to the UNIX user
            LOG.debug("Unable to get login user before Kerberos auth attempt.", e);
        }

        // we already have logged in successfully
        if (curUser != null && curUser.getUserName().equals(principal)) {
            LOG.debug("{}: Using existing principal ({}): {}",
                    new Object[]{sink, principal, curUser});

            // no principal found
        } else {

            LOG.info("{}: Attempting kerberos login as principal ({}) from keytab " +
                    "file ({})", new Object[]{sink, principal, keytab});

            // attempt static kerberos login
            UserGroupInformation.loginUserFromKeytab(principal, keytab);
            curUser = UserGroupInformation.getLoginUser();
        }

        return curUser;
    }

    /**
     * 将文件路径中的特定占位符替换掉
     * @param e 事件
     * @return 替换占位符后的文件路径
     */
    private String transFilePath(Event e){
        Map<String, String> headers = e.getHeaders();
        String topic, appId;
        String resultFilePath = new String(this.filePath);

        topic = headers.get(KafkaFlumeConstans.HEADER_TOPIC_KEY);
        appId = headers.get(KafkaFlumeConstans.HEADER_APP_ID_KEY);

        //获取Topic
        topic = topic == null ? KafkaFlumeConstans.HEADER_TOPIC_KEY : topic;
        //获取APPID
        appId = appId == null ? KafkaFlumeConstans.DEFAULT_APP_ID : appId;

        //替换topic和appid的占位符
        resultFilePath = resultFilePath.replace("$topic$", topic);
        resultFilePath = resultFilePath.replace("$appid$", appId);

        LOG.debug("[hy]real file path is " + resultFilePath);
        return resultFilePath;
    }

    @Override
    public String toString() {
        return "{ Sink type:" + getClass().getSimpleName() + ", name:" + getName() + " }";
    }

    @VisibleForTesting
    void setBucketClock(Clock clock) {
        BucketPath.setClock(clock);
    }


}
