package com.huayu.metis.tasks;

import com.huayu.metis.options.ScheduleOption;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * Metis的作业构建器
 * Created by Administrator on 14-5-14.
 */
public class JobBuilder {

    private Job job;
    private SimpleDateFormat simpleFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");

    public JobBuilder() {

    }

    public JobBuilder create(String jobName) {
        job = new Job(jobName);
        return this;
    }

    public JobBuilder setTask(BasicTask task) {
        job.getTasks().add(task);
        return this;
    }

    /**
     * 添加只执行一次的Schedule
     * @param name Schedule的名称
     * @param executeDate 执行的时间,精确到分钟
     * @return Builder对象
     */
    public JobBuilder setOnceSchedule(String name, Date executeDate) {
        Properties conf = new Properties();
        conf.setProperty(Schedule.SCHEDULE_TYPE, "once");
        conf.setProperty(Schedule.SCHEDULE_EXECUTE_TIME, simpleFormat.format(executeDate));

        Schedule schedule = new Schedule(name, conf);
        job.getSchedules().add(schedule);
        return this;
    }

    /**
     * 添加按照固定间隔时间重复执行的Schedule
     * @param name Schedule名称
     * @param start 开始日期
     * @param end 结束日期
     * @param spiltMinute 执行间隔分钟数
     * @param neverEnd 是否会结束
     * @return
     */
    public JobBuilder setRepeatEachDaySpiltSchedule(String name, Date start, Date end,
                                        int spiltMinute, boolean neverEnd) {
        Properties conf = new Properties();
        conf.setProperty(Schedule.SCHEDULE_TYPE, "repeat");
        conf.setProperty(Schedule.SCHEDULE_START_DATE, simpleFormat.format(start));
        conf.setProperty(Schedule.SCHEDULE_END_DATE, simpleFormat.format(end));
        conf.setProperty(Schedule.SCHEDULE_NEVER_END, String.valueOf(neverEnd));
        conf.setProperty(Schedule.SCHEDULE_FREQUENCY_TYPE, "spilt");
        conf.setProperty(Schedule.SCHEDULE_SPILT_MINUTE, String.valueOf(spiltMinute));

        Schedule schedule = new Schedule(name, conf);
        job.getSchedules().add(schedule);

        return this;
    }

    /**
     * 添加每天一次重复执行的Schedule
     * @param name Schedule名称
     * @param start 开始日期
     * @param end 结束日期
     * @param executeDate 执行的时间,精确到分钟
     * @param neverEnd 是否会结束
     * @return
     */
    public JobBuilder setRepeatEachDayOnceSchedule(String name, Date start, Date end,
                                                   Date executeDate, boolean neverEnd) {

        Properties conf = new Properties();
        conf.setProperty(Schedule.SCHEDULE_TYPE, "repeat");
        conf.setProperty(Schedule.SCHEDULE_START_DATE, simpleFormat.format(start));
        conf.setProperty(Schedule.SCHEDULE_END_DATE, simpleFormat.format(end));
        conf.setProperty(Schedule.SCHEDULE_NEVER_END, String.valueOf(neverEnd));
        conf.setProperty(Schedule.SCHEDULE_FREQUENCY_TYPE, "once");
        conf.setProperty(Schedule.SCHEDULE_EXECUTE_TIME, simpleFormat.format(executeDate));

        Schedule schedule = new Schedule(name, conf);
        job.getSchedules().add(schedule);

        return this;
    }

    public Job build() {
        if(job != null){
            return job;
        }
        throw new NullPointerException("please call create() first");
    }
}
