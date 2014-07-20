package com.huayu.metis.jobs;

import com.huayu.metis.options.TaskOption;
import com.huayu.metis.tasks.BasicTask;
import com.huayu.metis.tasks.Task;
import com.huayu.metis.tasks.TaskResult;

import java.util.Map;

/**
 * Created by Administrator on 14-5-16.
 */
public class DemoMetisTask extends BasicTask {

    public DemoMetisTask(TaskOption option) {
        super(option);
    }

    @Override
    public Map getTaskConfig() {
        return null;
    }

    @Override
    public void prepare(Map conf) {

    }

    @Override
    public TaskResult execute() {
        int counter = 0, max = 100000;
        try {
            while (counter < max) {
                System.out.println("This is Demo Counter..." + counter);
                ++counter;
                Thread.sleep(150);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            return TaskResult.Cancelled;
        }

        return TaskResult.Finish;
    }

    @Override
    public void clean() {

    }

    @Override
    public Task getNextTask() {
        return null;
    }
}
