package com.huayu.metis.tasks;

import com.huayu.metis.options.TaskOption;

import java.util.Map;

/**
 * Created by Administrator on 14-5-14.
 */
public abstract class BasicTask implements Task {

    private TaskOption option;

    public BasicTask(TaskOption option) {
        this.option = option;
    }

    public TaskOption getOption() {
        return option;
    }

    @Override
    public abstract void prepare(Map conf);
    @Override
    public abstract TaskResult execute();
    @Override
    public abstract void clean();
}
