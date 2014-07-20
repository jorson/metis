package com.huayu.metis.tasks;

import java.util.Map;

/**
 * Created by Administrator on 14-5-14.
 */
public interface Task {

    public void prepare(Map conf);

    public TaskResult execute();

    public void clean();

    public Task getNextTask();

    public Map getTaskConfig();
}
