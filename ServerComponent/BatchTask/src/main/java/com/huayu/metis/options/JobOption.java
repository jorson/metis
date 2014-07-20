package com.huayu.metis.options;

import com.google.gson.annotations.SerializedName;

import java.util.List;

/**
 * Created by Administrator on 14-5-28.
 */
public class JobOption {

    @SerializedName("jobName")
    private String jobName;
    @SerializedName("schedules")
    private List<ScheduleOption> scheduleOptions;
    @SerializedName("tasks")
    private List<TaskOption> taskOptions;

    public JobOption() {

    }

    public String getJobName() {
        return jobName;
    }

    public List<ScheduleOption> getScheduleOptions() {
        return scheduleOptions;
    }

    public List<TaskOption> getTaskOptions() {
        return taskOptions;
    }
}
