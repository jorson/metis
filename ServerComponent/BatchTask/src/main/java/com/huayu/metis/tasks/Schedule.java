package com.huayu.metis.tasks;

import com.google.common.base.Preconditions;
import com.huayu.metis.options.ScheduleOption;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;

/**
 * 执行时间控制
 * Created by Administrator on 14-5-14.
 */
class Schedule {

    private String scheduleName;
    //计划开始日期
    private Date startDate;
    //计划结束日期
    private Date stopDate;
    //是否无结束日期
    private boolean neverEnd = true;
    //执行类型, 重复执行/执行一次
    private ScheduleOption.ScheduleType scheduleType = ScheduleOption.ScheduleType.Repeat;
    //每日执行频率, 每日执行一次/间隔N分钟执行一次
    private ScheduleOption.DailyFrequencyType dailyFrequencyType = ScheduleOption.DailyFrequencyType.ExecuteOnce;
    //执行一次的时间点
    private Date executeTime;
    //间隔执行的分钟数, 转为Long
    private long spiltMinute;
    //不执行类型
    private ScheduleOption.ExecuteExceptType exceptType = ScheduleOption.ExecuteExceptType.None;
    //不执行序列
    private int[] exceptSequence;

    private final long oneMinute = 1000 * 59;
    private final SimpleDateFormat simpleFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");

    private Properties config;

    //配置项的Key
    public static final String SCHEDULE_START_DATE = "schedule.start.date";
    public static final String SCHEDULE_END_DATE = "schedule.end.date";
    public static final String SCHEDULE_NEVER_END = "schedule.never.end";
    public static final String SCHEDULE_TYPE = "schedule.type";
    public static final String SCHEDULE_FREQUENCY_TYPE = "schedule.frequency.type";
    public static final String SCHEDULE_EXECUTE_TIME = "schedule.execute.time";
    public static final String SCHEDULE_SPILT_MINUTE = "schedule.spilt.minute";

    public Schedule(String scheduleName, Properties conf) {
        this.scheduleName = scheduleName;
        this.config = conf;

        loadConfig();
    }

    /**
     * 检查一个时间点,是否为需要执行任务的时间点
     * @param checkDate 检查日期
     * @return 是否需要执行
     */
    public boolean needRun(Date checkDate) {
        //先检查执行的类型

        //如果是只执行一次的
        if(scheduleType == ScheduleOption.ScheduleType.Once) {
            return isAroundOneMinute(executeTime, checkDate);
        }
        //如果是需要重复执行的
        //如果需要检查的时间小于开始时间, 还没有到要开始的时间
        if(startDate.getTime() > checkDate.getTime()) {
            return false;
        }
        //如果不是从不结束, 且结束时间小于检查时间, 表示已经结束了
        if(!neverEnd && stopDate.getTime() < checkDate.getTime()){
            return false;
        }
        //先处理每天执行一次的
        if(dailyFrequencyType == ScheduleOption.DailyFrequencyType.ExecuteOnce) {
            Date todayExecute = getTodayExecutePoint(executeTime);
            return isAroundOneMinute(todayExecute, checkDate);

        }
        //需要重复执行的
        else if (dailyFrequencyType == ScheduleOption.DailyFrequencyType.ExecuteSpilt) {
            //以开始时间点为起点和检查时间点间的数据差,能否被间隔所整除
            long checkDateLong = checkDate.getTime();
            long startDateLong = startDate.getTime();

            long mod = (checkDateLong - startDateLong) % spiltMinute;
            //如果检查时间与标准时间的差, 与固定时间间隔 相除的余数为 0 到 59999 即在1分钟之内
            if(mod >=0 && mod <= 59999) {
                return true;
            } else {
                return false;
            }
        }
        return false;
    }

    /**
     * 检查Time1时间和Time2的时间差距是否在1分钟内
     * @param time1 要检查的时间
     * @param time2 标准时间
     * @return 检查结果,在1分钟内为true,反之为false
     */
    private boolean isAroundOneMinute(Date time1, Date time2) {
        long longTime1 = time1.getTime();
        long longTime2 = time2.getTime();
        //如果检查时间大于等于标准时间, 且小于标准时间+59S
        if(longTime1 >= longTime2 && longTime1 <= longTime2 + oneMinute) {
            return true;
        }
        return false;
    }

    /**
     * 获取指定日期在今天的执行点
     * @param setPoint 设置日期
     * @return 今日执行点
     */
    private Date getTodayExecutePoint(Date setPoint) {
        Calendar calendar = Calendar.getInstance();
        Calendar setCalender = Calendar.getInstance();
        setCalender.setTime(setPoint);

        calendar.set(calendar.get(Calendar.YEAR),
                calendar.get(Calendar.MONTH),
                calendar.get(Calendar.DAY_OF_MONTH),
                setCalender.get(Calendar.HOUR_OF_DAY),
                setCalender.get(Calendar.MINUTE),
                setCalender.get(Calendar.SECOND));

        return calendar.getTime();
    }

    private void loadConfig() {
        try {
            this.scheduleType = ScheduleOption.ScheduleType.getEnum(Preconditions.checkNotNull(
                    config.getProperty(SCHEDULE_TYPE), "schedule type is require"));
            //如果是执行一次
            if(this.scheduleType == ScheduleOption.ScheduleType.Once) {
                this.executeTime =  simpleFormat.parse(Preconditions.checkNotNull(
                        config.getProperty(SCHEDULE_EXECUTE_TIME), "execute time is require"));
            }
            //如果是重复执行
            else if(this.scheduleType == ScheduleOption.ScheduleType.Repeat) {
                //获取执行开始, 结束时间
                this.startDate =  simpleFormat.parse(Preconditions.checkNotNull(
                        config.getProperty(SCHEDULE_START_DATE), "start time is require"));
                this.neverEnd = Boolean.parseBoolean(Preconditions.checkNotNull(config.getProperty(SCHEDULE_NEVER_END),
                        "never end is require"));
                if(!this.neverEnd) {
                    this.stopDate =  simpleFormat.parse(Preconditions.checkNotNull(
                            config.getProperty(SCHEDULE_START_DATE), "stop time is require"));
                }

                //获取重复执行的频率参数
                this.dailyFrequencyType = ScheduleOption.DailyFrequencyType.getEnum(
                        Preconditions.checkNotNull(config.getProperty(SCHEDULE_FREQUENCY_TYPE),
                                "execute frequency not null"));
                //每天执行一次
                if(this.dailyFrequencyType == ScheduleOption.DailyFrequencyType.ExecuteOnce) {
                    this.spiltMinute = Long.parseLong(Preconditions.checkNotNull(config.getProperty(SCHEDULE_SPILT_MINUTE),
                            "execute spilt minute is require"));
                }
                //每隔一段时间执行
                else if(this.dailyFrequencyType == ScheduleOption.DailyFrequencyType.ExecuteSpilt) {
                    this.executeTime =  simpleFormat.parse(Preconditions.checkNotNull(
                            config.getProperty(SCHEDULE_EXECUTE_TIME), "execute time is require"));
                }
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
