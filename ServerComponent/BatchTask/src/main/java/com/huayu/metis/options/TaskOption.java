package com.huayu.metis.options;

import com.google.gson.annotations.SerializedName;
import com.huayu.metis.tasks.BasicTask;

/**
 * Created by Administrator on 14-5-16.
 */
public class TaskOption {

    @SerializedName("name")
    private String taskName;
    @SerializedName("package")
    private String packagePath;
    @SerializedName("taskClass")
    private String taskClass;
    @SerializedName("onSuccess")
    private String onSuccess;
    @SerializedName("onFailure")
    private String onFailure;

    private AfterTaskBehavior successBehavior = AfterTaskBehavior.FinishAndReportSuccess;
    private AfterTaskBehavior failureBehavior = AfterTaskBehavior.FinishAndReportFailure;

    private Class<? extends BasicTask> executeTaskClass;

    public TaskOption() {

    }

    public void setTaskName(String name) {
        this.taskName = name;
    }

    public void setPackagePath(String packagePath) {
        this.packagePath = packagePath;
    }

    public void setTaskClass(String classPath) {
        this.taskClass = classPath;
    }

    public void setSuccessBehavior(AfterTaskBehavior behavior) {
        this.successBehavior = behavior;
    }

    public void setFailureBehavior(AfterTaskBehavior behavior) {
        this.failureBehavior = behavior;
    }

    public String getTaskName() {
        return taskName;
    }

    public String getPackagePath() {
        return packagePath;
    }

    public String getTaskClass() {
        return taskClass;
    }

    public AfterTaskBehavior getSuccessBehavior() {
        return successBehavior;
    }

    public AfterTaskBehavior getFailureBehavior() {
        return failureBehavior;
    }

    public Class<? extends BasicTask> getExecuteTaskClass() {
        return executeTaskClass;
    }

    public void setExecuteTaskClass(Class<? extends BasicTask> executeTaskClass) {
        this.executeTaskClass = executeTaskClass;
    }
}
