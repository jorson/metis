package com.huayu.metis.jobs;

import com.huayu.metis.options.AfterTaskBehavior;
import com.huayu.metis.options.TaskOption;
import com.huayu.metis.tasks.*;
import junit.framework.Assert;
import net.sourceforge.groboutils.junit.v1.MultiThreadedTestRunner;
import net.sourceforge.groboutils.junit.v1.TestRunnable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.*;

/**
 * Created by Administrator on 14-5-14.
 */
public class SimpleJobTest {

    JobBuilder builder;
    ExecutorService executorService;
    Job job;

    @Before
    public void setup() {
        builder = new JobBuilder();
        executorService = Executors.newSingleThreadExecutor();
    }

    @After
    public void clean() {
        if(executorService != null){
            executorService.shutdown();
            executorService = null;
        }
    }

    @Test
    public void jobScheduleSettingTest() {
        Calendar calendar = Calendar.getInstance();
        Calendar start = Calendar.getInstance();
        Calendar stop = Calendar.getInstance();

        calendar.set(2014, 5, 28, 15, 15);
        start.set(2014, 5, 28);
        stop.set(2014, 6, 28);

        Job demoJob = builder.create("ScheduleJob")
                .setOnceSchedule("OnceSchedule", calendar.getTime())
                .setRepeatEachDayOnceSchedule("OnceEachDay", start.getTime(),
                        stop.getTime(), calendar.getTime(), false)
                .setRepeatEachDaySpiltSchedule("RepeatSpiltMinute", start.getTime(),
                        stop.getTime(), 10, false)
                .build();

        Assert.assertNotNull(demoJob);
        Assert.assertEquals(demoJob.getSchedules().size(), 3);
    }

    @Test
    public void jobTaskSettingTest() throws Exception {
        TaskOption option = buildTaskOption();
        TaskBuilder taskBuilder = new TaskBuilder(option);
        BasicTask task = taskBuilder.build();

        builder = new JobBuilder();
        Job job = builder.create("TestJob").setTask(task).build();
        Assert.assertNotNull(job);
    }

    @Test
    public void taskBuilderTest() throws Exception {
        TaskOption option = buildTaskOption();
        TaskBuilder builder = new TaskBuilder(option);
        BasicTask task = builder.build();
        Assert.assertNotNull(task);
    }

    public void jobRunTest() throws Exception {
        TaskOption option = buildTaskOption();
        TaskBuilder taskBuilder = new TaskBuilder(option);
        BasicTask task = taskBuilder.build();

        builder = new JobBuilder();
        Job job = builder.create("TestJob").setTask(task).build();

        job.startJob();
    }

    public void jobStopTest() throws Exception {
        TaskOption option = buildTaskOption();
        TaskBuilder taskBuilder = new TaskBuilder(option);
        BasicTask task = taskBuilder.build();

        builder = new JobBuilder();
        job = builder.create("TestJob").setTask(task).build();

        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try{
                    Thread.currentThread().sleep(1000);
                } catch(Exception ex) {
                    ex.printStackTrace();
                }
            }
        });

        job.startJob();
    }

    private TaskOption buildTaskOption() {
        TaskOption option = new TaskOption();

        option.setTaskName("TestTask");
        option.setFailureBehavior(AfterTaskBehavior.FinishAndReportFailure);
        option.setSuccessBehavior(AfterTaskBehavior.FinishAndReportSuccess);
        option.setExecuteTaskClass(DemoMetisTask.class);
        return option;
    }

    private static class JobAgentTestThread extends TestRunnable {

        @Override
        public void runTest() throws Throwable {
        }
    }
}
