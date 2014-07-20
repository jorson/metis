package com.huayu.metis.tasks;

import com.huayu.metis.options.AfterTaskBehavior;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Date;
import java.util.Queue;
import java.util.concurrent.*;

/**
 * Created by Administrator on 14-5-14.
 */
public class Job {

    private String jobName;
    private JobStatus currentJobStatus = JobStatus.Ready;
    private Queue<BasicTask> tasks;
    private Queue<Schedule> schedules;

    private long taskRunTimeout = 1000;

    private ExecutorService runTimeoutPool;


    private static final int MAX_TASK_NUM = 100;
    private static final int MAX_SCHEDULE_NUM = 10;

    public Job(String jobName) {
        this.jobName = jobName;
        this.tasks =  new ConcurrentLinkedQueue<BasicTask>();
        this.schedules = new ConcurrentLinkedQueue<Schedule>();
        this.runTimeoutPool = Executors.newSingleThreadExecutor();
    }

    /**
     * 开始Job
     * @throws Exception
     */
    public void startJob() throws Exception {
        startJob(0);
    }

    /**
     * 从指定任务序号开始Job
     * @param taskIndex Task序列号, 从0开始
     * @throws Exception
     */
    public void startJob(int taskIndex) throws Exception {
        if(tasks == null || tasks.isEmpty()) {
            //TODO: 记录没有可以执行任务的日志信息
            return;
        }

        int skipTask = 0;
        TaskResult taskResult = TaskResult.Finish;
        for(final BasicTask task : tasks) {
            if(skipTask < taskIndex) {
                skipTask++;
                continue;
            }

            taskResult = runWithTimeout(new MetisTaskRunner() {
                @Override
                public TaskResult run() throws Exception {
                    return task.execute();
                }
            });

            if(taskResult == TaskResult.Timeout ||
                    taskResult == TaskResult.Failure) {
                //获取任务执行后的操作
                AfterTaskBehavior behavior = task.getOption().getFailureBehavior();
                //如果任务失败后,仍然继续
                if(behavior == AfterTaskBehavior.ContinueNext){
                    continue;
                } else {
                    //TODO:这里反馈Job的最终执行结果
                    break;
                }
            } else if(taskResult == TaskResult.Finish) {     //任务正常结束

            } else if(taskResult == TaskResult.ContinueNext) {

            }
        }
    }

    /**
     * 停止Job
     */
    public void stopJob() {
        if(runTimeoutPool == null){
            return;
        }
        System.out.println("Job will shut down!");
        runTimeoutPool.shutdown();
        try {
            while (runTimeoutPool.isTerminated() == false) {
                runTimeoutPool.awaitTermination(taskRunTimeout, TimeUnit.SECONDS);
            }
        } catch (InterruptedException ex) {
            //TODO:记录关闭Job的错误
        }

        //清理对象
        runTimeoutPool = null;
    }

    /**
     * 查看Job的历史执行消息
     * @param begin 开始查看的日期
     * @param end 结束查看的日期
     */
    public void viewHistory(Date begin, Date end) {

    }

    //获取作业名称
    public String getJobName() {
        return jobName;
    }
    //获取作业当前的状态
    public JobStatus getCurrentJobStatus() { return currentJobStatus; }
    //获取排程信息
    public Queue<Schedule> getSchedules() {
        return schedules;
    }
    //获取任务信息
    public Queue<BasicTask> getTasks() {
        return tasks;
    }


    void addTask(BasicTask task) {
        if(this.tasks.size() >= MAX_TASK_NUM) {
            this.tasks.poll();
        }
        this.tasks.add(task);
    }

    void addSchedule(Schedule schedule) {
        if(this.schedules.size() >= MAX_SCHEDULE_NUM) {
            this.schedules.poll();
        }
        this.schedules.add(schedule);
    }

    boolean needRun(Date checkDate) {
        if(schedules == null || schedules.size() == 0){
            return false;
        }
        //检查所有的排程, 如果其中有一个排程需要运行, 则开始执行
        for(Schedule schedule : schedules) {
            if(schedule.needRun(checkDate)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 执行JOb中的Task, 如果出现超时,则返回失败
     * @param runner 需要执行的Task
     * @return 执行Task的结果
     * @throws Exception
     */
    private TaskResult runWithTimeout(final MetisTaskRunner runner) throws Exception {
        Future<TaskResult> future = runTimeoutPool.submit(new Callable() {
            @Override
            public TaskResult call() throws Exception {
                return runPrivileged(new PrivilegedExceptionAction() {
                    @Override
                    public TaskResult run() throws Exception {
                        return runner.run();
                    }
                });
            }
        });

        try {
            //返回Task执行任务的结果
            return future.get(taskRunTimeout, TimeUnit.SECONDS);
        }
        //捕捉超时的错误
        catch (TimeoutException et) {
            //超时后取消掉执行中的任务
            future.cancel(true);
            //报告Task执行失败
            return TaskResult.Timeout;
        }
        catch (ExecutionException ee) {
            Throwable cause = ee.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            } else if (cause instanceof InterruptedException) {
                throw (InterruptedException) cause;
            } else if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            } else if (cause instanceof Error) {
                throw (Error) cause;
            } else {
                throw new RuntimeException(ee);
            }
        }
        catch (CancellationException ce) {
            return TaskResult.Cancelled;
        }
        catch (InterruptedException ex) {
            return TaskResult.Failure;
        }
    }

    private TaskResult runPrivileged(final PrivilegedExceptionAction<TaskResult> action) throws IOException, InterruptedException {
        try {
            return action.run();
        } catch (IOException ex) {
            throw ex;
        } catch (InterruptedException ex) {
            throw ex;
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException("Unexpected exception.", ex);
        }
    }

    private interface MetisTaskRunner {
        TaskResult run() throws Exception;
    }
}
