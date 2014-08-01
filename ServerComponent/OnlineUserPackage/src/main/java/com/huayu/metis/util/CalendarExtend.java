package com.huayu.metis.util;

import java.util.Calendar;

/**
 * Created by Administrator on 14-8-1.
 */
public class CalendarExtend {

    public static Long[] getWeekStartEnd(Calendar in) {
        Long[] results = new Long[2];
        Calendar calendar = Calendar.getInstance();
        calendar.set(in.get(Calendar.YEAR),
                in.get(Calendar.MONTH),
                in.get(Calendar.DAY_OF_MONTH), 0, 0, 0);
        calendar.set(Calendar.MILLISECOND, 0);

        int weekDay = calendar.get(Calendar.DAY_OF_WEEK);
        if(weekDay == Calendar.SUNDAY) {
            calendar.add(Calendar.DATE, 6);
        } else {
            calendar.add(Calendar.DATE, 2 - weekDay);
        }

        results[0] = calendar.getTimeInMillis();
        calendar.add(Calendar.DATE, 6);
        results[1] = calendar.getTimeInMillis();
        return  results;
    }

    public static Long[] getMonthStartEnd(Calendar in) {
        Long[] results = new Long[2];
        Calendar calendar = Calendar.getInstance();
        calendar.set(in.get(Calendar.YEAR),
                in.get(Calendar.MONTH),
                in.get(Calendar.DAY_OF_MONTH), 0, 0, 0);
        calendar.set(Calendar.MILLISECOND, 0);

        calendar.set(Calendar.DATE, calendar.getActualMinimum(Calendar.DAY_OF_MONTH));
        results[0] = calendar.getTimeInMillis();

        calendar.set(Calendar.DATE, calendar.getActualMaximum(Calendar.DAY_OF_MONTH));
        results[1] = calendar.getTimeInMillis();

        return results;
    }
}
