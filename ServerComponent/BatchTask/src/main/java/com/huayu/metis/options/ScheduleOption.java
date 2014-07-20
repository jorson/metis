package com.huayu.metis.options;

import com.google.gson.annotations.SerializedName;

import java.util.Date;

/**
 * Created by Administrator on 14-5-27.
 */
public class ScheduleOption {

    @SerializedName("name")
    private String scheduleName;
    //计划开始日期
    @SerializedName("startDate")
    private String startDate;
    //计划结束日期
    @SerializedName("stopDate")
    private String stopDate;
    //是否无结束日期
    @SerializedName("neverEnd")
    private boolean neverEnd;
    //执行类型, 重复执行/执行一次
    @SerializedName("type")
    private String scheduleType;
    //每日执行频率, 每日执行一次/间隔N分钟执行一次
    @SerializedName("dailyFrequency")
    private String dailyFrequencyType;
    //执行一次的时间点
    @SerializedName("executeTime")
    private String executeTime;
    //间隔执行的分钟数, 转为Long
    @SerializedName("spiltMinute")
    private long spiltMinute;

    public String getScheduleName() {
        return scheduleName;
    }

    public String getStartDate() {
        return startDate;
    }

    public String getStopDate() {
        return stopDate;
    }

    public boolean isNeverEnd() {
        return neverEnd;
    }

    public String getScheduleType() {
        return scheduleType;
    }

    public String getDailyFrequencyType() {
        return dailyFrequencyType;
    }

    public String getExecuteTime() {
        return executeTime;
    }

    public long getSpiltMinute() {
        return spiltMinute;
    }

    /**
     * 每日频率类型
     */
    public enum DailyFrequencyType {
        /**
         * 执行一次
         */
        ExecuteOnce("onec"),
        /**
         * 按间隔执行
         */
        ExecuteSpilt("spilt'");

        private String frequency;

        DailyFrequencyType(String frequency) {
            this.frequency = frequency;
        }

        @Override
        public String toString() {
            return frequency;
        }

        public static DailyFrequencyType getEnum(String type) {
            for (DailyFrequencyType t : DailyFrequencyType.values()) {
                if (type.equalsIgnoreCase(t.frequency)) {
                    return t;
                }
            }
            return null;
        }
    }
    /**
     * 执行类型
     */
    public enum ScheduleType {
        /**
         * 循环执行
         */
        Repeat("repeat"),
        /**
         * 执行一次
         */
        Once("once");

        private String type;

        ScheduleType(String repeat) {
            this.type = repeat;
        }

        @Override
        public String toString() {
            return type;
        }

        public static ScheduleType getEnum(String type) {
            for (ScheduleType t : ScheduleType.values()) {
                if (type.equalsIgnoreCase(t.type)) {
                    return t;
                }
            }
            return null;
        }
    }
    /**
     * 不执行类型
     */
    public enum ExecuteExceptType {
        /**
         * 不存在不执行情况(默认)
         */
        None,
        /**
         * 在一周内不执行的天, 1~7
         */
        InWeek,
        /**
         * 在一月内不执行的天, 1~31
         */
        InMonth,
        /**
         * 在一天内不执行的小时, 0~23
         */
        InDay
    }

}
