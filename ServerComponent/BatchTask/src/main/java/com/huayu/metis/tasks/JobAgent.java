package com.huayu.metis.tasks;

import com.google.common.base.Preconditions;
import com.huayu.metis.options.JobAgentOption;

import java.util.*;
import java.util.concurrent.*;

/**
 * 任务的代理类, 用于管理作业, 执行作业. 为整个模块的统一入口
 * Created by Administrator on 14-5-14.
 */
public class JobAgent {

    private String agentName;
    private ConcurrentHashMap<String, Job> jobList;
    private Map<String, Future> jobFutureList;
    private Properties config;
    private static Object syncLocker = new Object();
    private static boolean isScannerRunning = false;
    private static boolean isCatcherRunning = false;

    public final static String AGENT_JOB_ROLL_POLING_MS = "agent.job.roll.ms";
    public final static String AGENT_CATCHER_POLING_MS = "agent.catcher.roll.ms";
    public final static String AGENT_MAX_JOB_THREAD = "agent.max.job.thread";

    private ExecutorService executorService;
    private Timer scannerTimer, catcherTimer;
    private JobAgentStatus agentStatus = JobAgentStatus.Stop;

    /*Agent的默认配置*/
    //默认Job轮询时间
    private final long defaultJobRollPoling = 1000 * 60;
    //默认Catcher轮询时间
    private final long defaultCatcherRollPoling = 1000 * 60;
    //默认单个Agent允许同时执行的Job数量
    private final int defaultMaxJob = 10;

    private JobAgentOption agentOption;

    /*Agent的配置*/
    private long jobRollPoling = defaultJobRollPoling;
    private long catcherRollPoling = defaultCatcherRollPoling;
    private int maxJobThread = 4;

    public JobAgent(String agentName, Properties conf) {
        this.agentName = agentName;
        this.jobFutureList = new HashMap<String, Future>();
        this.executorService = Executors.newFixedThreadPool(maxJobThread);
        this.scannerTimer = new Timer("job.scanner", true);
        this.catcherTimer = new Timer("job.catcher", true);
        this.config = conf;
        prepareJobAgent();
    }

    public JobAgent(JobAgentOption option) {
        this.agentOption = option;
    }

    /*公共的方法*/

    /**
     * 启动代理服务
     */
    public void startAgent() {
        //如果Agent处于停止状态
        if(agentStatus != JobAgentStatus.Stop) {
            return;
        }
        agentStatus = JobAgentStatus.Starting; //标记为启动中
        //启动扫描器
        scannerTimer.schedule(new JobScannerTask(), 10*1000, jobRollPoling);
        //启动作业抓取
        catcherTimer.schedule(new JobCatcherTask(), 5*1000, catcherRollPoling);
        agentStatus = JobAgentStatus.Start;  //标记为启动
    }

    /**
     * 停止代理服务
     * @throws InterruptedException
     */
    public void stopAgent() throws InterruptedException {
        //如果Agent不处于启动状态, 也就不需要停止
        if(agentStatus != JobAgentStatus.Start) {
            return;
        }
        //设置为正在停止
        agentStatus = JobAgentStatus.Stopping;
        //关闭启动作业的线程
        executorService.shutdown();
        //停止轮询
        scannerTimer.cancel();
        //停止所有在运行中的作业
        for(Map.Entry<String, Job> jobEntry : jobList.entrySet()) {
            Job job = jobEntry.getValue();
            if(job.getCurrentJobStatus() == JobStatus.Running) {
                job.stopJob();
            }
        }
        //关闭执行器线程
        while (!executorService.isTerminated()) {
            //停止所有的作业
            executorService.awaitTermination(300, TimeUnit.SECONDS);
        }
        //TODO:记录日志

        //清理对象
        scannerTimer = null;
        executorService = null;
        jobList.clear();
        jobFutureList.clear();
        //设置Agent的状态
        agentStatus = JobAgentStatus.Stop;
    }

    /**
     * 在Agent中注册作业
     * @param job 需要注册到Agent的作业
     */
    public void registerJob(Job job) {
        //如果Agent处于启动状态时
        if(agentStatus != JobAgentStatus.Start) {
            return;
        }

        //检查在JobList中是否存在
        if(!jobList.containsKey(job.getJobName())) {
            //开始注册作业
            synchronized (syncLocker) {
                if(!jobList.containsKey(job.getJobName())) {
                    jobList.put(job.getJobName(), job);
                }
            }
        }
    }

    /**
     * 从Agent中反注册作业
     * @param jobName 要反注册的作业名称
     */
    public void unregisterJob(String jobName) throws InterruptedException {
        //如果Agent处于启动状态时
        if(agentStatus != JobAgentStatus.Start) {
            return;
        }

        //检查在JobList中是否存在
        if(jobList.containsKey(jobName)) {
            //如果存在, 再检查在运行状态
            Job job = jobList.get(jobName);
            //如果Job是处于运行状态的
            if(job.getCurrentJobStatus() == JobStatus.Running) {
                //获取Job的Future
                Future future = jobFutureList.get(jobName);
                //强行停止作业
                if(future != null) {
                    future.cancel(true);
                }
                //等待处理完成
                while (!future.isCancelled()) {
                    Thread.sleep(500);
                }
            }
            //从Job列表中移除
            synchronized (syncLocker) {
                jobList.remove(jobName);
                jobFutureList.remove(jobName);
            }
        }
    }

    public void disableJob() {

    }

    public void viewJobHistory(String jobName) {

    }

    /*非公共方法*/
    private void prepareJobAgent() {
        String strJobRollPoling = config.getProperty(AGENT_JOB_ROLL_POLING_MS);
        if(strJobRollPoling != null) {
            this.jobRollPoling = Long.parseLong(strJobRollPoling);
        }
        String strCatcherRollPoling = config.getProperty(AGENT_CATCHER_POLING_MS);
        if(strJobRollPoling != null) {
            this.catcherRollPoling = Long.parseLong(strCatcherRollPoling);
        }
        String strMaxJobThread = config.getProperty(AGENT_MAX_JOB_THREAD);
        if(strJobRollPoling != null) {
            this.maxJobThread = Integer.parseInt(strMaxJobThread);
        }
    }

    /**
     * 作业轮询器,定时轮询所有需要在Agent上运行的Job,</br>如果有Job需要运行,则启动之
     */
    private class JobScannerTask extends TimerTask {

        @Override
        public void run() {
            if(isScannerRunning) {
                return;
            }
            //标记扫描器为正在运行, 不允许重复扫描
            isScannerRunning = true;

            try {
                //在检查期间不能对Job进行添加和删除
                synchronized (syncLocker) {
                    Date now = new Date();

                    for(Map.Entry<String, Job> jobEntry : jobList.entrySet()) {
                        Job job = jobEntry.getValue();
                        JobStatus status = job.getCurrentJobStatus();
                        //如果当前作业处于准备就绪的状态
                        if(status != JobStatus.Running) {
                            //检查作业是否需要运行
                            if(job.needRun(now)) {
                                beginJob(job.getJobName());
                            }
                        }
                    }
                    //标记扫描器停止运行
                    isScannerRunning = false;
                }
            } catch (Exception ex) {
                //TODO: 记录错误信息
                ex.printStackTrace();
            }
        }

        //在新线程中启动作业
        private void beginJob(final String jobName) {
            //执行器
            Runnable runnable = new Runnable() {
                @Override
                public void run() {
                    try {
                        Job job = jobList.get(jobName);
                        if(job != null) {
                            job.startJob();
                        }
                    } catch (Exception e) {
                        //TODO: 记录日志并推出
                        e.printStackTrace();
                    }
                }
            };
            Future future = executorService.submit(runnable);
            jobFutureList.put(jobName, future);
        }
    }

    /**
     * 定时轮询,从作业管理平台上获取分配给Agent的作业请求
     * */
    private class JobCatcherTask extends TimerTask {

        @Override
        public void run() {
            if(isCatcherRunning) {
                return;
            }
            //标记Catcher正在运行
            isCatcherRunning = true;

            try {
                //Catcher和Scanner之间的操作是互斥的
                synchronized (syncLocker) {

                }
            }  catch (Exception ex) {
                //TODO: 记录错误信息
                ex.printStackTrace();
            }
        }
    }
}
